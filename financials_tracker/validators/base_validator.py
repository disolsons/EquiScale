from abc import ABC, abstractmethod
from typing import Any


class BaseValidator(ABC):
    """
    Base class for all validators.

    Validators should implement a validate() method that returns
    a structured dictionary report.
    """

    @abstractmethod
    def validate(self, *args, **kwargs) -> dict[str, Any]:
        pass