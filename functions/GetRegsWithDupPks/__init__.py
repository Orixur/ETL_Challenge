import json
import logging
from os import dup
from typing import Dict, List, Union, Tuple

from ..utils.db_manager import DBManager

ID_QUERY_INDEX = 0
MUESTRA_QUERY_INDEX = 1
RESULTADO_QUERY_INDEX = 2


class WrongOutputError(Exception):
    pass


def main(kwargs: Dict[str, Union[Dict[str, str], List[List[str]]]]) -> List[List[list]]:
    """
        This activity will retrieve all the requiered information from each register for each duplicated pk,
        in order to update those registers leaving one of them as the most recent one.\n
        The expected response from the database is a list of tuples with the following schema:\n
        +-------------+\n
        | FECHA_COPIA |\n
        +-------------+\n
        +-FIXED-ORDER-+\n
        +-------------+\n
        | 1           |\n
        +-------------+\n

        Args:
            kwargs (dict): In binding with schema:
                db_config (dict):
                    database (str): Type of database wanted to initialize a new DBManager instance
                    table (str): Desired table to query within selected database
                dup_pks (list): A list of lists, for which, it item will be a duplicated composed pk
        
        Returns:
            list: The returned value will be a list of lists, for which, each item will have two more lists:
                - The first one will be the duplicated composed pk
                - The second one will be a list of strs (because the only data required to update fields are the ones on "FECHA_COPIA" field)
    """
    dbm = DBManager(_type=kwargs['db_config']['database'])
    res = []

    for dup_pk in kwargs['dup_pks']:
        regs = dbm.execute_sql_command(
            """
                SELECT 
                    FECHA_COPIA,
                    MOST_RECENT
                FROM {table}
                WHERE
                    ID='{id}' AND MUESTRA='{muestra}' AND RESULTADO='{resultado}'
            """.format(table=kwargs['db_config']['table'], id=dup_pk[ID_QUERY_INDEX], muestra=dup_pk[MUESTRA_QUERY_INDEX], resultado=dup_pk[RESULTADO_QUERY_INDEX]),
            ret=True
        )

        # If any register among results for a duplicated pk has MOST_RECENT field value set as True (1 in BIT SQL Server type)
        # It means no operation needs to be done in that group, therefore no operation will be recorded for any of the registers
        # for the given duplicated composed pk
        if True in set([r[1] for r in regs]):
            continue
        else:
            # I have to iterate every reg in regs, because response is tuple for every matching row
            # r[0] = Value for FECHA_COPIA, as it is the only field retrieved from query
            # r[0] is casted from datetime to string because activity functions can only return JSON serializable objects
            _regs = [str(r[0]) for r in regs]
            # If no registers where founded for a duplicated composed pk, a bug may arised over codebase
            if not _regs:
                raise WrongOutputError(f'Zero results where founded for duplicated composed pk {dup_pk}.')
            res.append([dup_pk, _regs])

    return res
