from ..interfaces.validor_interface import ValidatorInterface
from ..core.processing_context import ProcessingContext
import pandas as pd

import logging

logger = logging.getLogger(__name__)


class SchemaValidator(ValidatorInterface):
    def check_validation(self, df, current_schema, context: ProcessingContext = None)->ValidatorInterface:
        schema = context.config["schema"]
        logger.debug(schema)
        for col, expected_type in schema.items():
            if col not in df.columns:
                continue
            actual_dtype = str(df[col].dtype)

            
            if expected_type == "string" and not pd.api.types.is_string_dtype(df[col]):
                logger.error(f"Column {col} is not string type")
                self.error.append(f"Column {col} is not string type")
                self.go_ahead = False
            elif expected_type == "float" and not pd.api.types.is_float_dtype(df[col]):
                
                logger.error(f"Column {col} is not float type")
                self.error.append(f"Column {col} is not float type")
                self.go_ahead = False
            elif (
                expected_type == "datetime"
                and not pd.api.types.is_datetime64_any_dtype(df[col])
            ):
                logger.error(f"Column {col} is not datetime type")
                self.error.append(f"Column {col} is not datetime type")
                self.go_ahead = False
        return self


class ColumnValidator(ValidatorInterface):
    def check_validation(self, df, current_schema, context: ProcessingContext = None)->ValidatorInterface:
        logger.debug(f"{current_schema=}")
        file_columns = set(current_schema['column_name'])
        schema:dict = context.config["schema"]
        if file_columns != set(schema.keys()):
            self.error.append(f"expected schema {set(schema.keys())} and data schema {file_columns} doesn't match ")
            self.go_ahead = False
        
        return self

