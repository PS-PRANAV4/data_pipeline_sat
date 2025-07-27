from ..interfaces.loading_interfaces import DataLoadingInterface
from ..core.processing_context import ProcessingContext
import logging
import duckdb

logger = logging.getLogger(__name__)


class ParquetLoader(DataLoadingInterface):
    def can_load(self, destination: str, context: ProcessingContext):
        logger.info(f"checking can we load the file {destination}")
        if destination is not None and destination.endswith(".parquet"):
            return self
        logger.info(
            f"can't load the file into duckdb and stopiing the loading file {destination}"
        )
        return False

    def load(self, destination, context):
        logger.info(f"started loading file {destination}")
        if destination == None:
            return None,None
        try:
            schema = duckdb.sql(f"DESCRIBE SELECT * FROM '{destination}'").df()
            logger.info(f"file {destination} schema loaded to duckdb")
            data_frame = duckdb.sql(f"SELECT * FROM '{destination}'").df()

            return (schema, data_frame)
        except Exception as e:
            logger.error(
                {"file": destination, "error": f"Schema inspection failed: {e}"}
            )
            return None, None
