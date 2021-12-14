# Description
This is an implementation of the challenge provided in the technical interview by Pi Data Strategy & Consulting.

# Running this application

## Starting functions app

The whole project was developed and tested on VSCode.

In order to run this functions app you have to install Core Tools in your system and the Azure functions VSCode extension.

After this you have to manually setup local.settings.json to start the functions app:

````json
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "MyStorageConnectionAppSetting": "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1",
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "SQL_DRIVER_SERVER": "...",
    "SQL_DRIVER_USERNAME": "...",
    "SQL_DRIVER_PASSWORD": "...",
    "SQL_DRIVER_WORKING_DATABASE": "Testing_ETL",
    "SQL_DRIVER_TESTING_DATABASE": "Testing", // Despite the name this database have the executions logs table
    "BAK_PROVIDED": "/var/opt/mssql/backup/Testing_ETL.bak"
  }
}
````

Is required by the app to setup all the variables listed in the previous.

## Firing up database and Azurite

To start SQL Server 2019 instance and Azurite local emulator Docker is required.

After initializing Docker runtime type the following command:

````bash
````



# Manually test Durable Functions
## Trigger starter
The endpoint needed to request an execution of an orchestrator is: http://localhost:7071/api/orchestrators/{orchestrator name}
The response will be the id of the orchestration and all the endpoints to query status of the created orchestration.