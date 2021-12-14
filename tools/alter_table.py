import os
import pyodbc


class EnvVariablesError(Exception):
    pass


class AlterDatabaseError(Exception):
    pass


class AlterDB:
    def __init__(self):
        self.server = os.environ.get('SQL_DRIVER_SERVER', None)
        self.username = os.environ.get('SQL_DRIVER_USERNAME', None)
        self.password = os.environ.get('SQL_DRIVER_PASSWORD', None)
        self.db = os.environ.get('SQL_DRIVER_WORKING_DATABASE', None)

        if not self.server or not self.username or not self.password or not self.db:
            raise EnvVariablesError(f'Environment variable missing. Check setup: server={self.server}, username={self.username}, password={self.password}, db={self.db}')
        
        print('All variable environments were properly setup.')
    
    def create_connection(self) -> None:
        print('Creating connection')
        return pyodbc.connect('DRIVER={SQL Server}' + f';SERVER={self.server};DATABASE={self.db};UID={self.username};PWD={self.password}')
    
    def execute_sql_command(self, sql: str, ret: bool = False) -> None:
        with self.create_connection() as conn:
            with conn.cursor() as curr:
                print(f'Executing command: {sql}')
                curr.execute(sql)

                if ret:
                    return curr.fetchall()

    def validate(self, col: str) -> bool:
        r = self.execute_sql_command("""select *
        from INFORMATION_SCHEMA.COLUMNS
        where TABLE_NAME='Unificado'""", ret=True)
        cols = [x[3] for x in r]

        return col in cols

    def run(self) -> None:
        new_col = 'MOST_RECENT'
        table = 'Testing_ETL.dbo.Unificado'
        if not self.validate(new_col):
            self.execute_sql_command(f"ALTER TABLE {table} ADD {new_col} BIT NOT NULL CONSTRAINT BIT_def DEFAULT 0")

            if self.validate(new_col):
                print('New column successfully created!')
            else:
                raise AlterDatabaseError('The new column could not been created..')
        else:
            print(f'New column {new_col} already exists in selected table: {table}')


if __name__ == '__main__':
    rdb = AlterDB()
    rdb.run()
