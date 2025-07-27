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
        full_path = os.path.join(os.getcwd(), 'dlake', file_path)
        
        os.makedirs(full_path, exist_ok=True)
        
        if not file_name.endswith(f'.{file_format}'):
            file_name = f'{file_name}.{file_format}'
        
        complete_file_path = os.path.join(full_path, file_name)
        
        mode = 'wb' if isinstance(file_content, bytes) else 'w'
        
        with open(complete_file_path, mode, encoding='utf-8' if mode == 'w' else None) as f:
            f.write(file_content)
    
    def retrieve_data(self, file_name: str) -> bytes:
        """
        Retrieve raw file content from the local data lake.

        :param file_name: Name of the file to retrieve.
        :return: Raw file content as bytes.
        :raises: FileNotFoundError if the file doesn't exist
        """
        if file_name.startswith('dlake/'):
            possible_paths = [
                os.path.join(os.getcwd(), file_name),
                os.path.join(os.path.dirname(os.getcwd()), file_name),
            ]
            
            complete_file_path = None
            for path in possible_paths:
                if os.path.exists(path):
                    complete_file_path = path
                    break
            
            if complete_file_path is None:
                file_name_only = file_name.split('/')[-1]
                for root, dirs, files in os.walk(os.getcwd()):
                    if file_name_only in files:
                        complete_file_path = os.path.join(root, file_name_only)
                        break
        else:
            complete_file_path = file_name
        
        if not os.path.exists(complete_file_path):
            raise FileNotFoundError(f"File {complete_file_path} not found in the data lake.")
        
        with open(complete_file_path, 'rb') as f:
            return f.read()
