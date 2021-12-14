import os
import pyodbc
import logging

from ..utils.db_manager import DBManager

def main(currentHash: str) -> bool:
    """
        This activity checks if the current hash was already loaded in the database.

        Args:
            currentHash (str): Hash of the current blob
        
        Returns:
            bool: True if the current hash exists within loaded hashes, otherwise False
    """
    dbm = DBManager(_type='test')

    unique_hashes_already_loaded = [t[0] for t in dbm.execute_sql_command("SELECT DISTINCT file_hash FROM Executions", ret=True)]

    return True if currentHash in unique_hashes_already_loaded else False
