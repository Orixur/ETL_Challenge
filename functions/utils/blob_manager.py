import os
import logging
from io import BytesIO
from typing import Dict

import pandas as pd
from azure.storage.blob import BlobServiceClient

# The MyStorageConnectionAppSetting environment variable is setted up in local.settings.json file
# regarding the functions app
STORAGE_CONNECTION_STRING = os.environ.get('MyStorageConnectionAppSetting', 'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;')
STORAGE_CONTAINER = 'intermediate-results'


class BlobDoesNotExistError(Exception):
    pass


class BlobManager:
    """
        This class exists to refactor all upload and download functionalities againts blob storage.\n
        
        Attrs:
            self.blob_service_client (azure.storage.blob.BlobServiceClient): This is the base client
                needed to perform the other operations
    """
    def __init__(self):
        self.blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)

    def download_blob_as_df(self, container: str, blob: str) -> pd.DataFrame:
        """
            This functions intends to return a DataFrame based on a feather file
            previously uploaded to blob storage.

            Args:
                container (str): Container in where the .ftr blob is located
                blob (str): Name of the .ftr blob to recreate the dataframe
            
            Returns:
                pd.DataFrame: DataFrame recreated based on .ftr file in blob storage

            Raises:
                BlobDoesNotExistError: This exception is raised when the desired blob
                    does not exists within the blob storage designated container
        """
        container_client = self.blob_service_client.get_container_client(container)
        blob_client = container_client.get_blob_client(blob)

        if not blob_client.exists():
            raise BlobDoesNotExistError(f'The blob {blob} in container {container} does not exists.')

        data = blob_client.download_blob()
        buffer = BytesIO()
        data.download_to_stream(buffer)
        return pd.read_feather(buffer)

    def upload_by_chunks(self, df: pd.DataFrame, container: str, filename: str, chunk_size: int = 4*1024*1024) -> Dict[str, str]:
        """
            This functions intends to upload a partial result dataframe state to blob storage
            for future function activities usage.\n
            If the blob already exists, the proper is deleted and created from scratch with this
            execution data (this is to avoid incosistency within appended blob blocks).

            Args:
                df (pd.DataFrame): DataFrame to save into blob storage
                container (str): Destination blob container
                filename (str): New blob name for what blocks will be appended
                chunk_size (int, default=4mb): The chunks size for what the feather file buffer
                    will be read
            
            Returns:
                dict: A dictionary in with the destination container and blob name where the dataframe
                    was dumped is provided
        """
        container_client = self.blob_service_client.get_container_client(container)
        b_buff = BytesIO()
        df.to_feather(b_buff)
        
        chunks_counter = 0
        blob_client = container_client.get_blob_client(filename)
        if not blob_client.exists():
            logging.debug('Blob file does not exists in current container, creating a new one...')
            blob_client.create_append_blob()
        else:
            logging.debug(f'Blob {filename} already exists... Deleting to avoid data inconsistency by appending block blobs')
            blob_client.delete_blob()
            blob_client.create_append_blob()
        
        logging.debug(f'chunk_size: {chunk_size}')
        # After writing you need to position the pointer right in the start of the buffer to further reading
        b_buff.seek(0)
        while True:
            chunks_counter += 1
            logging.debug(f'Uploading chunk: {chunks_counter}')
            read_data = b_buff.read(chunk_size)
            if not read_data:
                logging.debug('Feather file uploaded')
                # The last "17 bytes" chunk is NOT uploaded due to EOF encounter (this is, b'' or empty byte with the object overhead)
                break
            blob_client.append_block(read_data)
        
        return {
            'container': STORAGE_CONTAINER,
            'blob': filename
        }
