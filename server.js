"usr strict"

var http = require('http');
var ODataServer = require("simple-odata-server");
var pg = require('pg');

var model = {
    namespace: "jsreport",
    entityTypes: {
        "SlipType": {
            "id": {"type": "Edm.String", key: true},
            "customercardid": {"type": "Edm.String"},
            "companyid": {"type": "Edm.String"},
            "storeid": {"type": "Edm.String"},
            "totalamount": {"type": "Edm.Double"},
            "enddatetime__c": {"type": "Edm.DateTimeOffset"}  //yyyy-MM-ddTHH:mm:ss.fffZ or yyyy-MM-ddTHH:mm:ss.fff[+&#124;-]HH:mm 2015-04-15T10:30:09.7550000Z
        }
    },
    entitySets: {
        "public.slip": {
          entityType: "jsreport.SlipType",
        //    type: "jsreport.AccountType",
        }
    }
};

console.log ('Connecting to : ' + process.env.PG_URL);
client.connect(process.env.PG_URL, function(err, client, done) {
  if(err) {
    return console.error('could not connect to postgres', err);
  } else {
		console.log ('Starting odata server on : ' + process.env.ODATA_HOSTNAME);
    var odataServer = ODataServer(process.env.ODATA_HOSTNAME)
      .model(model)
      .query((setName, query, cb) => {

				console.log ("query with " + JSON.stringify(query));
				client.query("SELECT id, customercardid, companyid, storeid, totalamount, enddatetime from public.slip", function(err, result) {
					done();
					if (err) {
						console.log ("query err " + JSON.stringify(err));
						cb(err);
					} else {

						console.log ("query succ " + JSON.stringify(result.rows));
						cb(null, {
								count: result.rows.length,
								value: result.rows
						});
					}
				});
      });
    http.createServer(odataServer.handle.bind(odataServer)).listen(process.env.PORT ||1337);
	}
});
