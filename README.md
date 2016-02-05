

###Make the Webjob ZIP
zip -r lib node_modules [push][pull]data.js



###webjob status
https://azureconnect.scm.azurewebsites.net/api/triggeredwebjobs/sync


set variables
export SF_CLIENTID=<salesforce oauth clientid>
export SF_CLIENT_SECRET=<salesforce oauth client secret>
export SF_USERNAME=<salesforce user>
export SF_PASSWORD=<salesforce pass>
export ODATA_HOSTNAME=<deployed host>
export REDIS_URL=<redis>
export SQL_USERNAME=<Azure Sql datadate username>
export SQL_PASSWORD=<Azure Sql datadate password>
export SQL_HOSTNAME=<Azure Sql datadate hostname>
export SQL_DBNAME=<Azure Sql datadate name>
