import logging
from typing import Dict, List, Tuple

from ..utils.db_manager import DBManager

GROUP_COUNT_QUERY_INDEX = 3

def main(kwargs: Dict[str, str]) -> List[Tuple[str, str, str, int]]:
    """
        This functions will query all groups by the implicit composed pk (ID, MUESTRA, RESULTADO).\n
        The expected response from the database is a list of tuples with the following schema:\n
        +----+---------+-----------+--------------------------------------+\n
        | ID | MUESTRA | RESULTADO | COUNT_OF_REGISTERS_PER_ID (NOT NULL) |\n
        +----+---------+-----------+--------------------------------------+\n
        +--------------------------FIXED-ORDER----------------------------+\n
        | 1  | 2       | 3         | 4                                    |\n
        +----+---------+-----------+--------------------------------------+\n

        Args:
            kwargs (dict): In binding with schema:
                database (str): Type of database wanted to initialize a new DBManager instance
                table (str): Desired table to query within selected database
        
        Returns:
            list: A list for which it item is a list representing duplicated composed pks
    """
    dbm = DBManager(_type=kwargs['database'])

    res = dbm.execute_sql_command(f'SELECT ID, MUESTRA, RESULTADO, COUNT(ID) FROM {kwargs["table"]} GROUP BY ID, MUESTRA, RESULTADO', ret=True)

    dup_pks = []
    for i in res:
        # If group count has more than a single occurence, it means that pk is repeated
        if i[GROUP_COUNT_QUERY_INDEX] > 1:
            # I have to cast tuple to list, because functions framework cannot convert it automatically to JSON payload
            # I take every item for each tuple, but excluding the last one (being the count per group)
            dup_pks.append(list(i[:-1]))
    
    return dup_pks
