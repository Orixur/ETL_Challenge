import uuid
import logging
from datetime import datetime
from typing import Dict

from ..utils.db_manager import DBManager

def main(kwargs: Dict[str, str]) -> None:
    """
        This activity will save an execution log within logging database.
        As the id is not handled by database, an uuid value is generated each time.

        Args:
            kwargs (dict): In binding with schema:
                update_resume (int): Number of updated rows
                inser_resume (int): Number of new rows
        
        Returns:
            str: String with all values used for the log (this string will be used for setting up custom orchestrator status)
    """
    dbm = DBManager(_type='test')
    id = str(uuid.uuid4())
    dt_now = datetime.now()

    # Retrieve unique set of composed pks within database (table="Unificado")
    db_unique_pks = """
    INSERT INTO Executions VALUES ('{id}', CAST('{dt_now}' AS DATETIME2), '{csv_blob_hash}', {resume_update}, {resume_insert})
    """.format(id=id, dt_now=dt_now, csv_blob_hash=kwargs['blob_hash'], resume_update=kwargs['update_resume'], resume_insert=kwargs['insert_resume'])

    _ = dbm.execute_and_commit_to_db([db_unique_pks])
    return str((id, str(dt_now), kwargs['blob_hash'], kwargs['update_resume'], kwargs['insert_resume']))
