import os

from datetime import datetime
from abc import ABC, abstractmethod

class Datalake(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def save(
        self,
        dataset: str,
        layer: str,
        file_path: str,
        file_name: str,
        file_content: bytes,
        file_format: str = 'json'
    ) -> None:
        """
        Abstract method to save data on the data lake.
        Args:
            dataset (str): Name of the dataset.
            layer (str): Layer of the data lake (e.g., bronze, silver, gold).
            file_name (str): Name of the file to save.
            file_content (bytes): Content of the file to save.
            file_format (str): Format of the file (default is 'json').
            file_path (str): Directory path where the file will be saved.
        """
        pass

    def save_on_storage(
        self,
        dataset: str,
        layer: str,
        file_name: str,
        file_content: bytes,
        file_format: str
    ) -> str:
        """
        Save data on the data lake.
        Args:
            dataset (str): Name of the dataset.
            layer (str): Layer of the data lake (e.g., bronze, silver, gold).
            file_name (str): Name of the file to save.
            file_content (bytes): Content of the file to save.
            file_format (str): Format of the file (default is 'json').
        Returns:
            str: Path to the saved file.
        """
        file_path = f"{layer}/{dataset}/year={datetime.now().year}/month={datetime.now().month}/day={datetime.now().day}"
        self.save(
            dataset=dataset, 
            layer=layer, 
            file_path=file_path, 
            file_name=file_name, 
            file_content=file_content,
            file_format=file_format
        )
        return f"{file_path}/{file_name}.{file_format}"

    def retrieve_data(self, file_name: str) -> bytes:
        """
        Retrieve raw file content from the local data lake.

        :param file_name: Name of the file to retrieve.
        :return: Raw file content as bytes.
        :raises: FileNotFoundError if the file doesn't exist
        """
        file_path = os.path.join(self.base_path, file_name)
        with open(file_path, 'rb') as f:
            return f.read()
        raise FileNotFoundError(f"File {file_name} not found in the data lake.")