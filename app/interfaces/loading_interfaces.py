from abc import ABC, abstractmethod


class DataLoadingInterface(ABC):
    """Abstract interface for data loading components"""

    @abstractmethod
    def can_load(self, destination: str, context: str) -> bool:
        """Check if this loader can handle the destination"""
        pass

    @abstractmethod
    def load(self, destination: str, context: str) -> bool:
        """Load data to destination"""
        pass
