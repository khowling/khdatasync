

###Make the Webjob
cp -r node_modules App_Data/jobs/triggered/sync
cp sync.js App_Data/jobs/triggered/sync/app.js
( cd App_Data/jobs/triggered/sync && zip -r pgsync app.js node_modules )



###webjob status
https://azureconnect.scm.azurewebsites.net/api/triggeredwebjobs/sync


set variables
export PG_URL=postgres://<user>:<password>@<host>/<db>
export SF_USERNAME=<sfuser>
export SF_PASSWORD=<sfpass>
export ODATA_HOSTNAME=<deployed host>
