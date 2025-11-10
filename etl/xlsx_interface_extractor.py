from abc import ABC, abstractmethod
from pandas import DataFrame

class IXlsxExtractor(ABC):
    @abstractmethod
    def is_valid_name(self, file_name: str) -> bool:
        pass

    @abstractmethod
    def transform(self, bucket_name: str, file_name: str) -> DataFrame:
        pass
