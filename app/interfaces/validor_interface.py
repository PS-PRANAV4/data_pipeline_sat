from abc import ABC, abstractmethod
from ..core.processing_context import ProcessingContext
import logging 

logger = logging.getLogger(__name__)

class ValidatorInterface(ABC):
    def __init__(self):
        self.error = None
        self.go_ahead = False
    
    @abstractmethod
    def check_validation(self, df, context: ProcessingContext = None) -> bool:
        """Implement  Validation here"""
        pass
    
    def confirm_go_ahead(self) -> bool:
        logger.error(f"{self.error}")
        return self.go_ahead
        

    
