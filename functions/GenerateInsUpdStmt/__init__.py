import logging
import pandas as pd
from typing import Dict, Union, List, Tuple

from ..utils.blob_manager import BlobManager

TABLE_PK = ['ID', 'MUESTRA', 'RESULTADO']

def build_df_upd_ins_str_format(cols: Union[List[str], Tuple[str]], sep: str = ',') -> str:
    """
        This function recreates the insert or update sections used by the SET, WHERE, INTO and VALUES SQL commands.\n
        The recreated format satisfies the interface provided by Pandas, since DataFrame is the data structure used in this solution.\n 
        The format will be '{0[<col_name>]}'\n
        The final format will be:\n
        \t- 'col_name = {0[<col_name>]}, col_name = {0[<col_name>]}, ...'\n
        Every {0[<col_name>]} will take the value of the column indicated for every row in where the format is rendered.

        Args:
            cols (list, tuple): An iterable with all the column names for which the format string will be rendered
            sep (str): The string used for join every format string into the final format to return

        Returns:
            str: Fully format string to build a SQL query string
    """
    aux = []
    for col in cols:
        aux.append(col + '={0[' + col + ']}')

    return sep.join(aux)

def build_proper_statement_for_each_row_in(table: str, pks: list, df: pd.DataFrame) -> pd.DataFrame:
    """
        This functions generates the insert or update query for each row in a dataframe.\n
        Una fila puede contener o un insert o un update.\n
        Every row will contain an insert query, and the ones previously identified will also have an update one.\n
        El dataframe objetivo debería tener un formato tal que:\n
        The argument dataframe should have an schema like (check the `SetStmtPerNewRow` activity function):
            +-------------------+-------------+----------------------------------------------+-------------+\n
            | <original fields> | MOST_RECENT | sql_stmt (agregada con check_pk_duplication) | insert_stmt |\n
            +-------------------+-------------+----------------------------------------------+-------------+\n

        Args:
            table (str): Name of the SQL table for which the queries will be created
            df_curr_values (pd.DataFrame): New data to prepare
            pks (list): List of defined pks within table schema

        Returns:
            pd.DataFrame: Se retorna el DataFrame recibido con una nueva columna, la cual contendrá la query de insert
                o update correspondiente a cada fila:
                The argument dataframe is returned with new columns added ():
                +-----------------------+-------------+----------+---------+---------+
                | <columnas originales> | MOST_RECENT | sql_stmt | ins_stm | upd_stm |
                +-----------------------+-------------+----------+---------+---------+
    """
    org_df_cols = df.drop(['insert_stmt', 'sql_stmt'], axis=1).columns

    cols = ','.join(org_df_cols)
    insert_str_fmt = f'INSERT INTO {table} ({cols}) VALUES ' + '{0[insert_stmt]};'
    _set = build_df_upd_ins_str_format(set(org_df_cols).difference(set(pks)))
    _where = build_df_upd_ins_str_format(pks, sep=' AND ')
    # update_str_fmt = f'UPDATE {table} SET {_set} WHERE {_where};'
    update_str_fmt = f'UPDATE {table} SET MOST_RECENT=0 WHERE {_where};'

    df['ins_stm'] = ''
    df['upd_stm'] = ''
    update_value = 'update'
    if update_value in set(df['sql_stmt']):
        df.loc[df['sql_stmt'] == update_value, 'upd_stm'] = df.loc[df['sql_stmt'] == update_value, :].agg(update_str_fmt.format, axis=1)

    df.loc[:, 'ins_stm'] = df.loc[:, :].agg(insert_str_fmt.format, axis=1)

    return df

def main(kwargs: Dict[str, str]) -> bool:
    """
        This activity will gather all the insert and update queries previously rendered.
        Considerations:\n
        - Data within the provided csv per week DOES NOT CONTAIN duplicated composed pks\n
        \n\t-If this occurs, this implies that the provided dataset is malformed and therefore the execution would stop
        - Any register within blob csv data will be newer than the existing one in db (if is the case), therefore, all the
            new registers will be setted up as "MOST_RECENT"

        Args:
            kwargs (dict): In binding with schema:
                - df (dict): {'container': str, 'blob': str}
                - table (str): SQL table name
    """
    # Retrieve df from feather blob (instructions in kwargs)
    bm = BlobManager()
    df = bm.download_blob_as_df(kwargs['df']['container'], kwargs['df']['blob'])

    # Assemble insert and update statements
    df = build_proper_statement_for_each_row_in(kwargs['table'], TABLE_PK, df)

    # Return the assembled statements
    res = {
        'insert_stmts': df[~(df['ins_stm'] == '')]['ins_stm'].values.tolist(),
        'update_stmts': df[~(df['upd_stm'] == '')]['upd_stm'].values.tolist() if 'update' in set(df['sql_stmt']) else []
    }
    return res
