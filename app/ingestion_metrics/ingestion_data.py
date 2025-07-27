from ..interfaces.ingestion_interfaces import IngestionInterface
import logging

logger = logging.getLogger(__name__)


class IngetionData(IngestionInterface):
    def run_metrics(self, path_of_file):
        logger.info("runiing inngestion metrics")
        stats = (
            self.conn.sql(f"""
            SELECT
                COUNT(*) AS total_rows,
                COUNT(*) - COUNT(sensor_id) AS missing_sensor_id,
                COUNT(*) - COUNT(value) AS missing_value
            FROM '{path_of_file}'
        """)
            .df()
            .iloc[0]
            .to_dict()
        )

        logger.debug(f"{stats=}")

        duplicate_Stats = (
            self.conn.sql(f"""
            WITH duplicates AS (
                SELECT timestamp, reading_type, COUNT(*) AS cnt
                FROM '{path_of_file}'
                GROUP BY timestamp, reading_type
                HAVING COUNT(*) > 1
            )
            SELECT
                COALESCE(SUM(cnt) - COUNT(*), 0) AS duplicate_rows
            FROM duplicates
        """)
            .df()
            .iloc[0]
            .to_dict()
        )

        self.conn.sql(f"""
            INSERT INTO ingestion_log (file_name, ingest_ts, total_rows, missing_sensor_id, missing_value, duplicate_rows) 
            VALUES (
                '{self.path}', 
                NOW(), 
                {stats["total_rows"]}, 
                {stats["missing_sensor_id"]}, 
                {stats["missing_value"]},
                {duplicate_Stats["duplicate_rows"]}
            )
        """)

        data = self.conn.sql("SELECT * FROM ingestion_log").df()

        logger.debug(f"Ingestion log data:\n{data}")
        logger.info("Ingestion metric completed")
