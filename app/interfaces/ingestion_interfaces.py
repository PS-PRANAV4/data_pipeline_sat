from abc import ABC, abstractmethod
import duckdb


class IngestionInterface(ABC):
    def __init__(self, path):
        self.path = path
        self.con = None
        self.run_table_creation()

    def run_table_creation(self):
        self.conn = duckdb.connect(self.path)
        self.conn.sql("""
        CREATE TABLE IF NOT EXISTS ingestion_log (
        file_name TEXT,
        ingest_ts TIMESTAMP,
        total_rows BIGINT,
        missing_sensor_id BIGINT,
        missing_value BIGINT,
        duplicate_rows BIGINT
        
    )
        """)

    @abstractmethod
    def run_metrics(self, path_of_file):
        pass

    def disconnect(self):
        if self.conn:
            self.conn.close()

    def __del__(self):
        self.disconnect()
