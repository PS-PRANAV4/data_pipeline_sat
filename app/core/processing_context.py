from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass


@dataclass
class ProcessingContext:
    """Context object passed through the pipeline"""

    metadata: Dict[str, Any] = None
    config: Dict[str, Any] = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
        if self.config is None:
            self.config = {}
