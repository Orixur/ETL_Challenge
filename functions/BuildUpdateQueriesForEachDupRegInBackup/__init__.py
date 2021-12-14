import logging
from datetime import datetime
from typing import Dict, List

DATE_FORMAT = '%Y-%m-%d %H:%M:%S.%f'
PK_CONFIG = {
    'ID': {'type': 'str', 'index': 0},
    'MUESTRA': {'type': 'str', 'index': 1},
    'RESULTADO': {'type': 'str', 'index': 2},
    'FECHA_COPIA': {'type': 'datetime', 'index': 3}
}

def build_where_clause(identifier: List[str], sep: str = ' AND ') -> str:
    aux = []

    for field, f_index in PK_CONFIG.items():
        if f_index['type'] == 'datetime':
            aux.append(f"{field}=CAST('{identifier[f_index['index']]}' AS DATETIME2)")
        else:
            aux.append(f"{field}='{identifier[f_index['index']]}'")

    return sep.join(aux)

def build_update_stmt(pk: List[str], table: str, date: str, is_most_recent: bool) -> str:
    imr = 1 if is_most_recent else 0
    # I have to build the where statement with FECHA_COPIA value, because is the only
    # value to distinguish the most recent or the olders registers among results for a composed pk
    _where = build_where_clause(pk + [date])

    q_string = f'UPDATE {table} SET MOST_RECENT={imr} WHERE {_where}'
    return q_string

def main(kwargs: Dict[Dict[str, str], List[List[list]]]) -> bool:
    """
        This activity generates a query string for every most recent tuple
        among duplicated registers for a composed pk.\n
        The query string generated will have a WHERE clause with pk (ID, MUESTRA & RESULTADO fields)
        and the value for FECHA_COPIA field, as this is the only field from these four that can distinguish
        a register from another among all registers for a duplicated pk.

        Args:
            Args:
            kwargs (dict): In binding with schema:
                db_config (dict):
                    database (str): Type of database wanted to initialize a new DBManager instance
                    table (str): Desired table to query within selected database
                dup_data (list): A list of lists, for which, each item will have two more lists:
                    - The first one will be the duplicated composed pk
                    - The second one will be a list of strs (because the only data required to update fields are the ones on "FECHA_COPIA" field)
        
        Returns:
            list: A list with all update queries for every most recent register
    """
    queries = []
    for pk, dates in kwargs['dup_data']:
        # Cast Every date in dates, from string to datetime to order them
        dates_as_datetimes = list(map(lambda x: datetime.strptime(x, DATE_FORMAT), dates))
        date = sorted(dates_as_datetimes, reverse=True)[0]
        is_most_recent = True
            
        queries.append(build_update_stmt(pk, kwargs['db_config']['table'], date, is_most_recent))

    return queries
