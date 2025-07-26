import os

from typing import Union
from modules.storage.data_lake import Datalake

class LocalDataLake(Datalake):
    """
    Local data lake for storing and retrieving data.
    """

    def save(
        self,
        dataset: str,
        layer: str,
        file_path: str,
        file_name: str,
        file_content: Union[bytes, str],
        file_format: str
    ) -> None:
        """
        Save data to the local data lake.
        
        Args:
            dataset (str): Name of the dataset.
            layer (str): Layer of the data lake (e.g., bronze, silver, gold).
            file_name (str): Name of the file to save.
            file_content (bytes): Content of the file to save.
            file_format (str): Format of the file.
            file_path (str): Directory path where the file will be saved.
        """
        full_path = os.path.join(os.getcwd(), 'dlake', layer, dataset, file_path)
        
        os.makedirs(full_path, exist_ok=True)
        
        if not file_name.endswith(f'.{file_format}'):
            file_name = f'{file_name}.{file_format}'
        
        complete_file_path = os.path.join(full_path, file_name)
        
        mode = 'wb' if isinstance(file_content, bytes) else 'w'
        
        with open(complete_file_path, mode, encoding='utf-8' if mode == 'w' else None) as f:
            f.write(file_content)
