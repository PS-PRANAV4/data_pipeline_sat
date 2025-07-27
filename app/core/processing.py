from .processing_context import ProcessingContext
from ..interfaces.loading_interfaces import DataLoadingInterface
from ..validators.base_validator import BaseValidator
import logging
from ..interfaces.validor_interface import ValidatorInterface
from ..utils.load_class import (
    load_validation_classes,
    load_ingestion_classes,
    load_transform_class,
)
from ..storages.store_data import StoreData
from ..ingestion_metrics.ingestion_data import IngetionData
from ..data_trasnformation.cleaners.drop_duplicate import DropDublicate
from ..data_trasnformation.cleaners.drop_missing_row import DropMissing
from ..data_trasnformation.data_quality_checks.date_check import DateCheck
from ..data_trasnformation.derived_data.z_score import ZScore
from ..data_trasnformation.derived_data.caliibaration import Caliboration
from ..data_trasnformation.derived_data.anomalous_reading import AnomalousReading
from ..data_trasnformation.derived_data.generate_stats import DeriveStats
from ..data_trasnformation.derived_data.profile import ProfileData
import pandas as pd
import os

logger = logging.getLogger(__name__)
derived_data_logs = logging.getLogger("derived_fields")


class Processing:
    def __init__(
        self, destination: str, context: ProcessingContext, ingestion_destination: str
    ):
        self.desination = destination
        self.context = context
        self.data_frame = None
        self.schema = None
        self.ingestion_destination = ingestion_destination

    def load_data(self, LoaderClass: DataLoadingInterface):
        logger.info("loading started")
        loader_obj: DataLoadingInterface | bool = LoaderClass().can_load(
            self.desination, self.context
        )
        if loader_obj:
            schema, data = loader_obj.load(self.desination, self.context)
            if schema is None or data is None:
                raise Exception("Check the path file not found")
            self.data_frame = data
            self.schema = schema
            logger.debug(f"{self.schema=} \n {self.data_frame}")
        else:
            raise Exception("Check the path file not found")
        return self

    def validator(self):
        logger.info("initialising validors")
        logger.debug(f"{self.context.config['validator_classes']=}")

        list_validator_classes_list: list[ValidatorInterface] = [
            load_validation_classes(class_string)
            .check_validation(
                context=self.context, df=self.data_frame, current_schema=self.schema
            )
            .confirm_go_ahead(destination=self.desination)
            for class_string in self.context.config["validator_classes"]
        ]

        logger.info("validators classes inititalised and run all validation")
        logger.debug(f"{list_validator_classes_list=}")
        if not any(list_validator_classes_list):
            raise Exception("validation pipeline failed check the logs for the error")
        return self

    def ingestion_metrics(
        self,
    ):
        logger.info("ingestion classes are initialising and loading")
        [
            load_ingestion_classes(
                class_string=class_string, path=self.ingestion_destination
            ).run_metrics(path_of_file=self.desination)
            for class_string in self.context.config["ingestion_classes"]
        ]
        logger.info("ingestion classes are loaded and run completely")
        return self

    def transformation_classes(self):
        logger.info("loading and inititialising trasnformation classes")
        for trnasfor_class_string in self.context.config["transform_classes"]:
            self.data_frame = load_transform_class(
                class_string=trnasfor_class_string
            ).clean(df=self.data_frame)

        logger.debug(f"{self.data_frame}")
        logger.info("loaded and transformed transformation classes")
        return self

    def derived_fields(self):
        logger.info("running derived classes")
        derived_data = DeriveStats().clean(df=self.data_frame)
        derived_data_logs.info(
            f"derived data for {self.desination} = \n {derived_data}"
        )
        logger.info("completed derived classes")
        return self

    def profile(self):
        profile_data = ProfileData().clean(df=self.data_frame)
        profile_path = f"{self.context.config['profile_path']}{self.desination.split('/')[-1].split(".")[0]}.csv"
        directory = os.path.dirname(profile_path)
        os.makedirs(directory, exist_ok=True)

        profile_data.to_csv(profile_path, index=False)
        return self

    def save_partitioned_parquet(
        self,
    ):
        logger.info('saving data with partition')
        StoreData().store(
            df=self.data_frame,
            base_path=self.context.config["base_path"],
            partition_cols=self.context.config["partition_cols"],
            compression=self.context.config["compression"],
        )
