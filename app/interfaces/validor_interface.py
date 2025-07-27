from abc import ABC, abstractmethod
from ..core.processing_context import ProcessingContext
import logging

logger = logging.getLogger("validation_file")


class ValidatorInterface(ABC):
    def __init__(self):
        self.error: list = []
        self.go_ahead = True

    @abstractmethod
    def check_validation(
        self, df, context: ProcessingContext = None
    ) -> "ValidatorInterface":
        """Implement  Validation here"""
        pass

    def confirm_go_ahead(self, destination) -> bool:
        for single_errors in self.error:
            logger.error(f"{single_errors} destination= {destination}")
        return self.go_ahead
