from typing import Any, Union

def error(what, shd):
    return f"{what} must be a {shd} type, got {type(what)}"

class TypeValidator:
    """Class for validating data types of database fields."""

    @staticmethod
    def is_string(value: Any):
        if not isinstance(value, str):
            raise ValueError(f"{value} must be a {str} type, got {type(value)}")

    @staticmethod
    def is_integer(value: Any):
        if not isinstance(value, int):
            raise ValueError(f"{value} must be a {int} type, got {type(value)}")

    @staticmethod
    def is_float(value: Any):
        if not isinstance(value, float):
            raise ValueError(f"{value} must be a {float} type, got {type(value)}")

    @staticmethod
    def is_dict(value: Any):
        if not isinstance(value, dict):
            raise ValueError(f"{value} must be a {dict} type, got {type(value)}")

    @staticmethod
    def is_list(value: Any):
        if not isinstance(value, list):
            raise ValueError(f"{value} must be a {list} type, got {type(value)}")

    @staticmethod
    def validate(value: Any, expected_type: Union[type, str]) -> bool:
        """Validate if the value matches the expected type."""
        type_map = {
            'str': str,
            'int': int,
            'float': float,
            'dict': dict,
            'list': list,
        }
        if isinstance(expected_type, str):
            expected_type = type_map.get(expected_type, None)
        
        return isinstance(value, expected_type) if expected_type else False
