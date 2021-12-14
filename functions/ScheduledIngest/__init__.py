import json
import json
import logging
from datetime import timedelta

import azure.functions as func
import azure.durable_functions as df

URL = 'https://francisadbteststorage.blob.core.windows.net/challenge/nuevas_filas.csv?sp=r&st=2021-07-08T18:53:40Z&se=2022-12-09T02:53:40Z&spr=https&sv=2020-08-04&sr=b&sig=AK7dCkWE1xR28ktHfdSYU2RSZITivBQmv83U51pyJMo%3D'
DELIMITER = ','
QUOTECHAR = '"'
WORK_TABLE = 'Unificado'

def orchestrator_function(context: df.DurableOrchestrationContext):
    """
        This orchestrator workflow covers the ingest of an external csv blob via url.\n
        This orchestrator will be scheduled after the proper execution.\n
        Take into account that an shceduled orchestrator remains the same orchestration instance,
        and as so, the status will be either Running (if it is awaiting or running), Failed or Completed (if
        the orchestration instance is terminated from client).

        Args:
            This orchestrator does not consume any parameters
        
        Returns:
            This orchestrator does not return any value
    """
    current_log = ''
    logging.info('Check if current blob hash was already uploaded in logging database (Executions table)')
    blob_hash = yield context.call_activity('GetBlobHash', URL)
    current_blob_was_already_loaded = yield context.call_activity('CheckCurrentBlobHash', blob_hash)
    if not current_blob_was_already_loaded:
        logging.info('Retrieve and perform basic preparation activities over new data')
        df_location = yield context.call_activity('RetrieveAndSaveDf', {'url': URL, 'delimiter': DELIMITER, 'quotechar': QUOTECHAR})
        
        logging.info('Apply transformations to new data and saved it')
        df_with_transformations = yield context.call_activity('Transformations', df_location)
        
        logging.info('Get unique pks within databse working table')
        j_db_unique_pks = yield context.call_activity('GetUniqueSetOfPksFromDb', WORK_TABLE)
        db_unique_pks = json.loads(j_db_unique_pks)

        logging.info('Mark each row of new data as insert or update based on unique pks within database')
        df_stmts = yield context.call_activity('SetStmtPerNewRow', {'df': df_with_transformations, 'db_unique_pks': db_unique_pks['db_unique_pks']})
        
        logging.info('Create insert and update statements')
        insert_statements = yield context.call_activity('GenerateInsUpdStmt', {'df': df_stmts['df'], 'table': WORK_TABLE})

        logging.info('Upload insert and update statements')
        upload_resume = yield context.call_activity('UploadNewData', insert_statements)

        logging.info('Save a log about current execution')
        current_log = yield context.call_activity('SaveExecutionLog', {'blob_hash': blob_hash, 'insert_resume': upload_resume['insert_rows_uploaded'], 'update_resume': upload_resume['update_rows_uploaded']})
    else:
        current_log = (f'Current blob_hash {blob_hash} was previously uploaded.')

    logging.info('Re-schedule orchestrator')
    next_ingest = context.current_utc_datetime + timedelta(minutes = 1)
    context.set_custom_status({
        'current_status_description': current_log,
        'next_ingest': str(next_ingest)
    })
    # yield context.create_timer(next_cleanup)

    # context.continue_as_new(None)


main = df.Orchestrator.create(orchestrator_function)
