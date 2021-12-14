import logging
import json

import azure.functions as func
import azure.durable_functions as df

DB = 'work'
TABLE = 'Unificado'

def orchestrator_function(context: df.DurableOrchestrationContext):
    """
        This orchestrator assemble all the operations in order to implement the requirement:\n
        "Consultando con el cliente, nos cuenta que es posible que estas cosas sucedan como
        consecuencia de errores en el sistema que genera estos archivos, pero que siempre
        tomemos el ultimo registro que fue copiado, considerando que un registro será
        duplicado si los campos [ID], [MUESTRA] y [RESULTADO] son iguales en dos filas
        distintas."\n

        Args:
            This orchestrator does not consume any parameters
        
        Returns:
            This orchestrator does not return any value
    """
    db_config = {'database': DB, 'table': TABLE}
    # Identify duplicated pks among database (Adding FECHA_COPIA field to distinguish them)
    dup_pks = yield context.call_activity('GetDupPksFromDb', db_config)

    # Gather all registers for each duplicated pk
    duplicated_registers_to_update = yield context.call_activity('GetRegsWithDupPks', {'db_config': db_config, 'dup_pks': dup_pks})

    if duplicated_registers_to_update:
        # Build update queries in order to satisfy requirement
        # DONE: Default value from schema for MOST_RECENT field should be 0, in order to fire an update just for the most recent ones
        query_strings = yield context.call_activity('BuildUpdateQueriesForEachDupRegInBackup', {'db_config': db_config, 'dup_data': duplicated_registers_to_update})

        # Fire the generated queries
        upload_resume = yield context.call_activity('UpdateBackupData', query_strings)
        return query_strings, upload_resume
    else:
        # duplicated_registers_to_update has no values, it means that all groups of registers for composed pks (duplicated values)
        # has a tuple with a True (1 in BIT SQL Server data type) in the MOST_RECENT field.
        # This means that no further update is needed
        return 'Registers are marked!\nNo upload operations had been done, because all groups of registers with a duplicated pk contains a tuple with a True (1 in BIT SQL Server data type) value for MOST_RECENT field'

main = df.Orchestrator.create(orchestrator_function)
