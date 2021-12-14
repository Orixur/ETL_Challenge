import os
import pyodbc


class EnvVariablesError(Exception):
    pass


class DatabaseCreationError(Exception):
    pass


class AlterDB:
    def __init__(self):
        self.server = os.environ.get('SQL_DRIVER_SERVER', None)
        self.username = os.environ.get('SQL_DRIVER_USERNAME', None)
        self.password = os.environ.get('SQL_DRIVER_PASSWORD', None)
        self.test_db = os.environ.get('SQL_DRIVER_TESTING_DATABASE', None)

        if not self.server or not self.username or not self.password or not self.test_db:
            raise EnvVariablesError(f'Environment variable missing. Check setup: server={self.server}, username={self.username}, password={self.password}, test_db={self.test_db}')
        
        print('All variable environments were properly setup.')
    
    def create_connection(self) -> None:
        print('Creating connection')
        return pyodbc.connect('DRIVER={SQL Server}' + f';SERVER={self.server};UID={self.username};PWD={self.password}')
    
    def execute_sql_command(self, sql: str, ret: bool = False) -> None:
        with self.create_connection() as conn:
            with conn.cursor() as curr:
                print(f'Executing command: {sql}')
                curr.execute(sql)

                if ret:
                    return curr.fetchall()

    def check_database_creation(self) -> None:
        query = """
        select [name] as database_name, 
            database_id, 
            create_date
        from sys.databases
        order by name
        """
        dbs_in_instance = self.execute_sql_command(query, ret=True)
        db_names = [x[0].strip().lower() for x in dbs_in_instance]

        return self.test_db.strip().lower() in db_names

    def run(self) -> None:
        if not self.check_database_creation():
            print('Starting logging database creation')
            query = """
            CREATE DATABASE {db}
            """.format(db=self.test_db)

            self.execute_sql_command(query)
            db_created = self.check_database_creation()

            if db_created:
                print('Logging database creation finished successfully')
            else:
                raise DatabaseCreationError(f'An error occurred during {self.test_db} databse creation... The database has not been created.')

            schema = """
            CREATE TABLE Testing.dbo.Executions (
                id VARCHAR(36) PRIMARY KEY,
                execution_date DATETIME,
                file_hash VARCHAR(500),
                rows_updated int,
                rows_inserted int
            )
            """
            self.execute_sql_command(schema)
        else:
            raise DatabaseCreationError(f'Database {self.restored_db} is already created in database.')

    


if __name__ == '__main__':
    rdb = AlterDB()
    rdb.run()
