import logging
from typing import Dict

from ..utils.blob_manager import BlobManager

DF_SQL_STMT_COL = 'sql_stmt'
DF_MOST_UPDATED_COL = 'MOST_RECENT'

def main(kwargs: Dict[str, str]) -> bool:
    """
        This activity will prepare the dataframe with extra information in order
        to prepare all the required queries to ingest.\n
        Steps:
        \t\n- Mark as "update" those rows with a pk within database
        \t\n- Mark the remianing rows as "insert", as they had not any match with database values
        \t\n- Mark every row as the MOST_RECENT one
        \t\n- Format datetime and object (string) dtype columns
        \t\n- Render the values section for the insert or update query

        Args:
            kwargs (dict): In binding with schema:
                - df (dict): {'container': str, 'blob': str}
                - table (str): SQL table name
                - db_unique_pks (list): List of tuples with all the composed pks values retrieved from database
        
        Returns:
            dict: A dictionary with dumped dataframe location in blob storage and all the duplicated pks
    """
    bm = BlobManager()

    # Get transformed new data (dataframe)
    df = bm.download_blob_as_df(kwargs['df']['container'], kwargs['df']['blob'])
    df[DF_SQL_STMT_COL] = ''

    dup_pks = []
    # Find all matching rows based on unique pks within database
    for db_pk in kwargs['db_unique_pks']:
        # Retrieve all row's indexes for any new data pk that matches a unique database pk
        i_rows_to_update = df.loc[(df['ID'] == db_pk[0]) & (df['MUESTRA'] == db_pk[1]) & (df['RESULTADO'] == db_pk[2])].index.tolist()
        
        # Mark each row as an 'update' sql statement (this will be used in the future to build the proper insert or update statements)
        if i_rows_to_update:
            dup_pks.append(db_pk)
            for i_row in i_rows_to_update:
                df.loc[i_row, DF_SQL_STMT_COL] = 'update'
    
    # Mark each remaining row with str('') as value in the new DF_SQL_STMT_COL field as an 'insert' sql statement (this will be used in the future to build the proper insert or update statements)
    df.loc[df[DF_SQL_STMT_COL] == '', DF_SQL_STMT_COL] = 'insert'
    
    # Those registers with update statement will have to update the older dbs
    # registers with MOST_UPDATED field as 0 (not the most updated register).
    # On the other hand, all the registers within the new data will be inserted, and so,
    # all these will be the newest ones.
    # Therefore... All the new registers need to be marked as the most recent ones
    df[DF_MOST_UPDATED_COL] = 1

    # Prepare object and datetime dtypes for sql statement
    for c in df.select_dtypes(include=['object']).columns:
        if c in ['insert_stmt', 'sql_stmt']:
            continue
        df[c] = df[c].map("'{}'".format)

    for c in df.select_dtypes(include=['datetime64']).columns:
        if c in ['insert_stmt', 'sql_stmt']:
            continue
        df[c] = df[c].map("CAST('{}' AS DATETIME2)".format)

    # Generate values statement for every row
    aux = []
    for c in df.columns:
        if c in ['insert_stmt', 'sql_stmt']:
            continue
        aux.append('{' + f'0[{c}]' + '}')
    agg_str = '(' + ','.join(aux) + ')'
    df['insert_stmt'] = df.agg(agg_str.format, axis=1)

    # Save the modified dataframe in blob storage
    return {'df': bm.upload_by_chunks(df, kwargs['df']['container'], 'df_stmts.ftr'), 'duplicated_pks': dup_pks}
