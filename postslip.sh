. ./ENV.sh

JS=`curl -X POST -d "username=${SF_USERNAME}&password=${SF_PASSWORD}" ${AFFINITY_URL}/authenticate`



SIG=`echo $JS | jq -r '.user.signature'`
TOKEN=`echo $JS | jq -r '.token'`

echo "$SIG $TOKEN"

curl -vX POST -d @khslip.json -H "Content-Type: application/json" -H "x-key: $SIG" -H "x-access-token: $TOKEN" ${AFFINITY_URL}/slip/inject
