from app.loading.parquet_loader import ParquetLoader
from app.core.processing import Processing
from app.utils.logger_config import setup_logging
import yaml
from app.core.processing_context import ProcessingContext
import logging
import duckdb
import os
from itertools import count
from pathlib import Path


if __name__ == "__main__":
    
    setup_logging()
    with open("config.yml", "r") as f:
        config = yaml.safe_load(f)
    with open("data/last_read/last_read.txt", "rb") as f:
        f.seek(0, 2)  
        filesize = f.tell()
        
        if filesize == 0:
            last_line = ""
        else:
            pos = filesize - 1
            while pos > 0:
                f.seek(pos)
                if f.read(1) == b'\n':
                    break
                pos -= 1
            f.seek(pos + 1)
            last_line = f.readline().decode().strip()

    print(f"Last line: {last_line}")
    find_line_no = last_line.split(".")[0].split("-")[1]
    print(find_line_no)
    
    processing_context_obj = ProcessingContext(config=config)
    for counter in count(int(find_line_no)):
        file_name = f"raw-{counter+1}.parquet"
        file = Path(f"{config["input_file_path"]}/{file_name}")
        
        if not file.exists():
            break 
        procesor_obj = (
            Processing(
                destination=str(file),
                context=processing_context_obj,
                ingestion_destination="output.duckdb",
            )
            .load_data(LoaderClass=ParquetLoader)
            .validator()
            .ingestion_metrics()
            .transformation_classes()
            .derived_fields()
            .profile()
            .save_partitioned_parquet()
        )
        
        with open("data/last_read/last_read.txt", "a") as f:
            f.write(f" \n{file_name}")






