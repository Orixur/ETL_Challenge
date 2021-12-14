# Description
This is an implementation of the challenge provided in the technical interview by Pi Data Strategy & Consulting.

# Running functions
There is two options in order to build up the functions app service:
- The first is by initializing the service through VSCODE
- The second one, consists on firing up the functions app with CLI command provided by Core Tools 

# Manually test Durable Functions
## Trigger starter
The endpoint needed to request an execution of an orchestrator is: http://localhost:7071/api/orchestrators/{orchestrator name}
The response will be the id of the orchestration and all the endpoints to query status of the created orchestration.