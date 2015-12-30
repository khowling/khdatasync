. ./env.sh

JS=`curl -X POST -d "username=${SF_USERNAME}&password=${SF_PASSWORD}" http://affinityapp.northeurope.cloudapp.azure.com:5000/authenticate`



SIG=`echo $JS | jq -r '.user.signature'`
TOKEN=`echo $JS | jq -r '.token'`

echo "$SIG $TOKEN"

curl -vX POST -d @khslip.json -H "Content-Type: application/json" -H "x-key: $SIG" -H "x-access-token: $TOKEN" http://affinityapp.northeurope.cloudapp.azure.com:5000/slip/inject
