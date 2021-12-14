import logging
from io import BytesIO
from typing import Dict

import pandas as pd
from azure.storage.blob import BlobServiceClient

from ..utils.blob_manager import BlobManager

STORAGE_CONTAINER = 'intermediate-results'
DTYPES = {
    'CHROM': 'str',
    'POS': 'str',
    'ID': 'str',
    'REF': 'str',
    'ALT': 'str',
    'QUAL': 'str',
    'FILTER': 'str',
    'INFO': 'str',
    'FORMAT': 'str',
    'MUESTRA': 'str',
    'VALOR': 'str',
    'ORIGEN': 'str',
    'FECHA_COPIA': 'datetime64[ns]',
    'RESULTADO': 'str'
}


class FieldNotFoundError(Exception):
    pass


def transform_df_dtypes(df: pd.DataFrame, dtypes: dict=DTYPES) -> None:
    """
        This function casts types per field.\n
        Modifications to dataframe are performed inplace, so no df is required to be returned.\n
        I created this functions because saving a casted dataframe (via df.astype()) to feather file
        does not save casted fields.
        
        Args:
            df (pd.DataFrame): Dataframe to modify
            dtypes (dict, default=__name__.DTYPES): Dtype per field configuration
        Raises:
            FieldNotFoundError: This exception is raised when one of the required fields is missing
    """
    for field, dtype in dtypes.items():
        if field not in df.columns:
            raise FieldNotFoundError(f'Field {field} not founded within columns of dataframe')
        df[field] = df[field].astype(dtype)

def main(kwargs: Dict[str, str]) -> bool:
    """
        Main function for this activity.\n
        This activity focuses on retrieving data from csv in the provided url.\n
        This also prepares dataframe dtypes and save it to feather file within blob storage.\n

        Args:
            kwargs: Dictionary of parameters provided by the orchestrator.
                The schema is:
                    - url: Remote data url
                    - delimiter: Remote data delimiter
                    - quotechar: Remote data quotechar
    """
    df = pd.read_csv(kwargs['url'], delimiter=kwargs['delimiter'], quotechar=kwargs['quotechar'])
    
    # Cast fields to proper data type, based on db schema
    logging.debug(f'Dataframe previous dtypes: {df.info()}')
    transform_df_dtypes(df)

    bm = BlobManager()
    filename = 'df.ftr'

    return bm.upload_by_chunks(df, STORAGE_CONTAINER, filename)
