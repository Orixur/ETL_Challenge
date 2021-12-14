import logging
from typing import List

from ..utils.db_manager import DBManager

def main(queries: List[str]) -> bool:
    """
        This activity will execute and commit every update previously generated.\n
        
        Args:
            queries (dict): A list of update queries
    """
    dbm = DBManager(_type='work')
    # Iterate and send requests for every update operation detected previously
    update_rows_uploaded = dbm.execute_and_commit_to_db(queries)

    # Iterate and send requests for every insert operation
    return {'update_rows_uploaded': update_rows_uploaded}
