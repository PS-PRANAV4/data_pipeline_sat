from app.loading.parquet_loader import ParquetLoader
from app.core.processing import Processing
from app.utils.logger_config import setup_logging
import yaml
from app.core.processing_context import ProcessingContext
import logging


if __name__ == "__main__":
    with open("config.yml", "r") as f:
        config = yaml.safe_load(f)
    processing_context_obj = ProcessingContext(config=config)
    setup_logging()
    procesor_obj = (
        Processing(
            destination="sample_sensor_data.parquet", context=processing_context_obj,
            ingestion_destination="output.duckdb"
        )
        .load_data(LoaderClass=ParquetLoader)
        .validator().ingestion_metrics()
    )
