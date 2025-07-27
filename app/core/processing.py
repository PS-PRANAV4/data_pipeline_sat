from .processing_context import ProcessingContext
from ..interfaces.loading_interfaces import DataLoadingInterface
from ..validators.base_validator import BaseValidator
import logging
from ..interfaces.validor_interface import ValidatorInterface
from ..utils.load_class import load_validation_classes

logger = logging.getLogger(__name__)


class Processing:
    def __init__(self, destination: str, context: ProcessingContext):
        self.desination = destination
        self.context = context
        self.data_frame = None
        self.schema = None

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
                context=self.context, df=self.data_frame, current_schema=""
            )
            for class_string in self.context.config["validator_classes"]
        ]

        logger.info("validators classes inititalised and run all validation")
        logger.debug(f"{list_validator_classes_list=}")
