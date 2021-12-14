import logging
import pandas as pd
from io import BytesIO
from typing import Dict
from datetime import datetime

from ..utils.blob_manager import BlobManager

def main(kwargs: Dict[str, str]) -> bool:
    """
        This activity funtion intends to transform data before uploading to the proper database.\n
        Transformations performed in this activity:\n
        1. Filling up "FECHA_COPIA" field with current datetime\n
        \n
        After all transformations are performed a partial result dataframe is saved within blob
        storage for further usage.

        Args:
            kwargs: Dictionary of parameters provided by the orchestrator.
                The schema is:
                    - container (str): Name of the container within the feather file blob is located
                    - blob (str): Name of the feather file blob
    """
    bm = BlobManager()
    df = bm.download_blob_as_df(kwargs['container'], kwargs['blob'])

    # Transformation 1 - Fill up "FECHA_COPIA" field with current datetime
    df['FECHA_COPIA'] = datetime.now()

    return bm.upload_by_chunks(df, kwargs['container'], 'df_transformed.ftr')
