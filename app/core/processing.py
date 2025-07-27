from .processing_context import ProcessingContext
from ..interfaces.loading_interfaces import DataLoadingInterface
from ..validators.base_validator import BaseValidator
import logging
from ..interfaces.validor_interface import ValidatorInterface
from ..utils.load_class import load_validation_classes,load_ingestion_classes
from ..ingestion_metrics.ingestion_data import IngetionData

logger = logging.getLogger(__name__)


class Processing:
    def __init__(self, destination: str, context: ProcessingContext,ingestion_destination:str):
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
            self.data_frame = data
            self.schema = schema
            logger.debug(f"{self.schema=} \n {self.data_frame}")
        return self

    def validator(self):
        logger.info("initialising validors")
        logger.debug(f"{self.context.config['validator_classes']=}")

        list_validator_classes_list: list[ValidatorInterface] = [
            load_validation_classes(class_string).check_validation(
                context=self.context, df=self.data_frame, current_schema=self.schema
            ).confirm_go_ahead(destination=self.desination)
            for class_string in self.context.config["validator_classes"]
        ]

        logger.info("validators classes inititalised and run all validation")
        logger.debug(f"{list_validator_classes_list=}")
        if not any(list_validator_classes_list):
            raise Exception("validation pipeline failed check the logs for the error")
        return self
        
    def ingestion_metrics(self,):
        [
            load_ingestion_classes(class_string=class_string,path=self.ingestion_destination).run_metrics(path_of_file=self.desination)
            for class_string in self.context.config["ingestion_classes"]
        ]
        return self
    def transformation_classes(self):
        pass
        

