import json
import logging

from ..utils.db_manager import DBManager

def main(workTable: str) -> str:
    """
        This activity will retrieve all unique pks for workTable.

        Args:
            workTable (str): Desired SQL table
        
        Returns:
            str: Json string with query result
    """
    dbm = DBManager(_type='work')

    # Retrieve unique set of composed pks within database (table="Unificado")
    db_unique_pks = "SELECT DISTINCT ID, MUESTRA, RESULTADO FROM {table}".format(db=dbm.work_db, table=workTable)

    r = dbm.execute_sql_command(db_unique_pks, ret=True)
    return json.dumps({'db_unique_pks': [tuple(t) for t in r]})