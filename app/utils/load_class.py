import importlib
from ..interfaces.validor_interface import ValidatorInterface
from ..interfaces.ingestion_interfaces import IngestionInterface


def load_class(full_class_string):
    module_path, class_name = full_class_string.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)


def load_validation_classes(full_class_string: str) -> ValidatorInterface:
    return load_class(full_class_string=full_class_string)()

def load_ingestion_classes(class_string:str,path)->IngestionInterface:
    return load_class(full_class_string=class_string)(path=path)
