import os
import pyodbc
import logging
from typing import Union, List


class EnvVariablesError(Exception):
    pass


class DBManager:
    def __init__(self, _type: str):
        self.server = os.environ.get('SQL_DRIVER_SERVER', None)
        self.username = os.environ.get('SQL_DRIVER_USERNAME', None)
        self.password = os.environ.get('SQL_DRIVER_PASSWORD', None)
        self.work_db = os.environ.get('SQL_DRIVER_WORKING_DATABASE', None)
        self.test_db = os.environ.get('SQL_DRIVER_TESTING_DATABASE', None)
        self._type = _type

        if not self.server or not self.username or not self.password or not self.work_db or not self.test_db:
            raise EnvVariablesError(f'Environment variable missing. Check setup: server={self.server}, username={self.username}, password={self.password}, work_db={self.work_db}, test_db={self.test_db}')
        
        logging.debug('All variable environments were properly setup.')
    
    def create_connection(self) -> pyodbc.Connection:
        logging.debug(f'Creating a {self._type} connection')
        if self._type == 'work':
            return pyodbc.connect('DRIVER={SQL Server}' + f';SERVER={self.server};DATABASE={self.work_db};UID={self.username};PWD={self.password}')
        elif self._type == 'test':
            return pyodbc.connect('DRIVER={SQL Server}' + f';SERVER={self.server};DATABASE={self.test_db};UID={self.username};PWD={self.password}')

    def execute_sql_command(self, sql: str, ret: bool = False) -> Union[None, list]:
        with self.create_connection() as conn:
            with conn.cursor() as curr:
                logging.debug(f'Executing command: {sql}')
                curr.execute(sql)

                if ret:
                    return curr.fetchall()
    
    def execute_and_commit_to_db(self, stmts: List[str]) -> None:
        counter = 0
        conn = self.create_connection()

        for stmt in stmts:           
            with conn.cursor() as curr:
                logging.debug(f'Executing command: {stmt}')
                curr.execute(stmt)
                curr.commit()
                logging.debug(f'Command commited')
            counter += 1

        return counter
