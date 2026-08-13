-- ============================================================================
-- dblog-lab: test database for experimenting with the DBLog algorithm in Jupyter.
-- Idempotent — can be run repeatedly without error.
-- ============================================================================

-- 1) Database dedicated to the lab
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'dblog_lab')
BEGIN
    PRINT 'Creating database dblog_lab...';
    CREATE DATABASE dblog_lab;
END
GO

USE dblog_lab;
GO

-- Required for the persisted computed column (total_amount) on dbo.sales.
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

-- 2) Enable CDC at the database level (prerequisite for sp_cdc_enable_table)
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'dblog_lab' AND is_cdc_enabled = 1)
BEGIN
    PRINT 'Enabling CDC on database dblog_lab...';
    EXEC sys.sp_cdc_enable_db;
END
GO

-- 3) Simple business table — the extraction target.
--    Columns deliberately varied (numeric, text, date, status) to give
--    insert/update/delete operations real material during the DBLog tests.
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'sales' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    PRINT 'Creating table dbo.sales...';
    CREATE TABLE dbo.sales (
        sale_id       INT IDENTITY(1,1) PRIMARY KEY,
        product_id    INT             NOT NULL,
        customer_id   INT             NOT NULL,
        quantity      INT             NOT NULL,
        unit_price    DECIMAL(10,2)   NOT NULL,
        total_amount  AS (CAST(quantity * unit_price AS DECIMAL(12,2))) PERSISTED,
        status        NVARCHAR(20)    NOT NULL DEFAULT N'PENDING',  -- PENDING | COMPLETED | CANCELLED
        sale_date     DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
        created_at    DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at    DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
        -- sys.types calls this 'timestamp', which is a row version and not a time at
        -- all: 8 opaque bytes. It reads as binary on both paths, and the tests say so
        -- against a real server rather than trusting the name.
        row_version   ROWVERSION      NOT NULL
    );
END
GO

-- 4) Enable CDC on the business table
IF NOT EXISTS (
    SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.sales')
)
BEGIN
    PRINT 'Enabling CDC on dbo.sales...';
    EXEC sys.sp_cdc_enable_table
        @source_schema        = N'dbo',
        @source_name          = N'sales',
        @role_name            = NULL,
        @capture_instance     = N'dbo_sales',
        @supports_net_changes = 1;
END
GO


-- 5) Pagination fixture — fixed content and a composite primary key, so the
--    chunked-read tests can assert exact key windows without depending on the
--    churn dbo.sales accumulates. No CDC: it is read-only scaffolding.
--    Column order is load-bearing: inspect() returns business_columns ordered by
--    column_id, and the tests assert both that order and the resulting dtypes.
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'pydblog_paging' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    PRINT 'Creating table dbo.pydblog_paging...';
    CREATE TABLE dbo.pydblog_paging (
        tenant_id  INT            NOT NULL,
        item_id    INT            NOT NULL,
        label      NVARCHAR(50)   NOT NULL,
        amount     DECIMAL(10,2)  NOT NULL,
        CONSTRAINT pk_pydblog_paging PRIMARY KEY CLUSTERED (tenant_id, item_id)
    );
END
GO

IF NOT EXISTS (SELECT 1 FROM dbo.pydblog_paging)
BEGIN
    PRINT 'Inserting fixture rows into dbo.pydblog_paging...';
    INSERT INTO dbo.pydblog_paging (tenant_id, item_id, label, amount) VALUES
        (1, 1, N't1i1',  10.10),
        (1, 2, N't1i2',  20.20),
        (1, 3, N't1i3',  30.30),
        (1, 4, N't1i4',  40.40),
        (1, 5, N't1i5',  50.50),
        (2, 1, N't2i1',  60.60),
        (2, 2, N't2i2',  70.70),
        (2, 3, N't2i3',  80.80),
        (2, 4, N't2i4',  90.90),
        (2, 5, N't2i5', 100.00);
END
GO


-- 6) Initial seed — data that already existed "before" the snapshot,
--    so the DBLog chunked-read phase has something to work on.
IF NOT EXISTS (SELECT 1 FROM dbo.sales)
BEGIN
    PRINT 'Inserting seed rows into dbo.sales...';
    INSERT INTO dbo.sales (product_id, customer_id, quantity, unit_price, status) VALUES
        (101, 1, 2,  350.00, N'COMPLETED'),
        (102, 2, 1, 1200.00, N'COMPLETED'),
        (103, 3, 3,   89.90, N'COMPLETED'),
        (104, 4, 1,  450.00, N'PENDING'),
        (105, 5, 1,  899.00, N'PENDING');
END
GO

-- 7) Schema drift fixture — CDC enabled, then a captured column dropped underneath it.
--    A dropped column is the drift SQL Server does not paper over: it stays in the
--    change table (unpopulated from then on) while the source loses it, so the log
--    read would project a column the table read cannot. inspect() has to refuse that.
--    Note a type change would NOT do: ALTER COLUMN is propagated to the change table,
--    so both sides stay in step and there is nothing to catch.
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'pydblog_drift' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    PRINT 'Creating table dbo.pydblog_drift...';
    CREATE TABLE dbo.pydblog_drift (
        drift_id     INT            NOT NULL IDENTITY(1,1),
        amount       DECIMAL(10,2)  NOT NULL,
        legacy_note  NVARCHAR(50)   NULL,
        CONSTRAINT pk_pydblog_drift PRIMARY KEY CLUSTERED (drift_id)
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.pydblog_drift')
)
BEGIN
    PRINT 'Enabling CDC on dbo.pydblog_drift...';
    EXEC sys.sp_cdc_enable_table
        @source_schema        = N'dbo',
        @source_name          = N'pydblog_drift',
        @role_name            = NULL,
        @capture_instance     = N'dbo_pydblog_drift',
        @supports_net_changes = 1;
END
GO

-- After CDC, deliberately: the change table has already captured the column.
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID('dbo.pydblog_drift') AND name = 'legacy_note'
)
BEGIN
    PRINT 'Dropping dbo.pydblog_drift.legacy_note to force schema drift...';
    ALTER TABLE dbo.pydblog_drift DROP COLUMN legacy_note;
END
GO

PRINT 'Setup complete.';
GO
