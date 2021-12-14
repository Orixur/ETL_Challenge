import os
import pyodbc


class EnvVariablesError(Exception):
    pass


class DatabaseRestoreError(Exception):
    pass


class RestoreDB:
    def __init__(self):
        self.server = os.environ.get('SQL_DRIVER_SERVER', None)
        self.username = os.environ.get('SQL_DRIVER_USERNAME', None)
        self.password = os.environ.get('SQL_DRIVER_PASSWORD', None)
        self.restored_db = os.environ.get('SQL_DRIVER_WORKING_DATABASE', None)
        self.bak_file = os.environ.get('BAK_PROVIDED', None)

        if not self.server or not self.username or not self.password or not self.restored_db or not self.bak_file:
            raise EnvVariablesError(f'Environment variable missing. Check setup: server={self.server}, username={self.username}, password={self.password}, restored_db={self.restored_db}, bak_file={self.bak_file}')
        
        print('All variable environments were properly setup.')
    
    def create_connection(self) -> None:
        print('Creating connection')
        return pyodbc.connect('DRIVER={SQL Server}' + f';SERVER={self.server};UID={self.username};PWD={self.password}', autocommit=True)

    def execute_restore_command(self, sql: str) -> None:
        with self.create_connection() as conn:
            with conn.cursor() as curr:
                print(f'Executing command: {sql}')
                curr.execute(sql)
                while (curr.nextset()):
                    pass
    
    def execute_sql_command(self, sql: str) -> None:
        with self.create_connection() as conn:
            with conn.cursor() as curr:
                print(f'Executing command: {sql}')
                curr.execute(sql)
                
                return curr.fetchall()
    
    def check_database_restore(self) -> None:
        query = """
        select [name] as database_name, 
            database_id, 
            create_date
        from sys.databases
        order by name
        """
        dbs_in_instance = self.execute_sql_command(query)
        db_names = [x[0].strip().lower() for x in dbs_in_instance]

        return self.restored_db.strip().lower() in db_names

    def restore_database(self) -> None:
        if not self.check_database_restore():
            print('Starting databse restoration')
            restore_filename = self.bak_file.split('/')[-1]
            mdf_file = f'/var/opt/mssql/data/{restore_filename.replace(".bak", ".mdf")}'
            ldf_file = f'/var/opt/mssql/data/{restore_filename.replace(".bak", ".ldf")}'

            restore = """
            RESTORE DATABASE {db} FROM DISK=N'{bak_file}'
            WITH MOVE '{db}' to '{mdf_file}',
            MOVE '{db}_log' to '{ldf_file}' 
            """.format(db=self.restored_db, bak_file=self.bak_file, mdf_file=mdf_file, ldf_file=ldf_file)

            self.execute_restore_command(restore)
            db_created = self.check_database_restore()

            if db_created:
                print('Database restoration finished successfully')
            else:
                raise DatabaseRestoreError(f'An error occurred during {self.restored_db} restore... The database has not been created.')
        else:
            raise DatabaseRestoreError(f'Database {self.restored_db} is already created in database.\nIn order to restore from .bak file the provided database name has not to been taken from an existing one.\nTry changing the desired database or drop the existing one.')


if __name__ == '__main__':
    rdb = RestoreDB()
    rdb.restore_database()
