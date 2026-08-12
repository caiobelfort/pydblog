from threading import local
from pydblog.connectors import build_connector
import logfire



if __name__ == "__main__":
    logfire.configure(send_to_logfire="if-token-present")

    connector = build_connector(
        host="localhost",
        port="1433",
        user="sa",
        password="Af1uente!LabPwd",
        database="dblog_lab",
        source_type="mssql"
    )


    connector.connect()

    spec = connector.inspect("dbo", "sales")

    print(spec)

    print(f" Min Lsn time for {spec.capture_instance}: {connector.map_lsn_to_timestamp(connector.get_min_lsn(spec.capture_instance))}")
    print(f" Max Lsn time for database: {connector.map_lsn_to_timestamp(connector.get_max_lsn())}")

    connector.close()
