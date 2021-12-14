import logging
from typing import Dict

from ..utils.db_manager import DBManager

def main(kwargs: Dict[str, str]) -> bool:
    """
        This activity will execute and commit every update and insert query
        previously generated.\n
        
        Args:
            kwargs (dict): A dictionary with all the updated and insert statements
    """
    dbm = DBManager(_type='work')
    # Iterate and send requests for every update operation detected previously
    update_rows_uploaded = dbm.execute_and_commit_to_db(kwargs['update_stmts'])
    insert_rows_uploaded = dbm.execute_and_commit_to_db(kwargs['insert_stmts'])

    # Iterate and send requests for every insert operation
    return {'update_rows_uploaded': update_rows_uploaded, 'insert_rows_uploaded': insert_rows_uploaded}