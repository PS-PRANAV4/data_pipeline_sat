from ..interfaces.validor_interface import ValidatorInterface
from typing import List


class BaseValidator:
    def __init__(self, validator_class_list: List[ValidatorInterface]):
        self.validator_list = validator_class_list

    def run_validation(self, context, df):
        for single_validator in self.validator_list:
            single_validator.check_validation(context=context, df=df)
