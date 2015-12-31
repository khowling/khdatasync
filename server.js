'use strict'

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
            "enddatetime": {"type": "Edm.DateTimeOffset"}  //yyyy-MM-ddTHH:mm:ss.fffZ or yyyy-MM-ddTHH:mm:ss.fff[+&#124;-]HH:mm 2015-04-15T10:30:09.7550000Z
				}
    },
    entitySets: {
        "slip": {
          entityType: "jsreport.SlipType"
        }
    }
};


//pg.defaults.poolSize = 5;
//pg.defaults.poolIdleTimeout = 3000; // 5 seconds (try to overcome the azure loadbalancer)
//pg.connect(process.env.PG_URL, function(err, client, done) {
//  if(err) {
//    return console.error('could not connect to postgres', err);
//  } else {
console.log ('Starting odata server on : ' + process.env.ODATA_HOSTNAME);
var odataServer = ODataServer(process.env.ODATA_HOSTNAME)
  .model(model)
  .query((setName, query, cb) => {
		let client = new pg.Client(process.env.PG_URL);
		console.log ('Connecting to (need to do it here because Azure cannot keep open a connection) : ' + process.env.PG_URL);

		client.connect(function(err) {
		  if(err) {
		    console.error('could not connect to postgres', err);
				cb(err);
		  } else {
				let qstr = `SELECT ${Object.keys(query['$select']).join(',')} FROM  ${query['collection']}`;
				if (query['$filter']) {
					let fkys = Object.keys(query['$filter']);
					if (fkys.length >0) {
						if (fkys[0] === "$or" || fkys[0] === "$and") {
							let ors = [], orvals = query['$filter'][fkys[0]];
							for (let f in orvals) {
								ors.push(` ${f} = '${orvals[f]}' `);
							}
							qstr+= ` WHERE ${ors.join(fkys[0].substring(1))}`;
						} else
							qstr+= ` WHERE ${fkys[0]} = '${query['$filter'][fkys[0]]}'`;
					}
				}
				if (query['$limit']) {
					qstr+= " limit " + query['$limit'];
				}
				console.log ("request with : " + JSON.stringify(query));
				console.log ("query with : " + qstr);
				client.query(qstr, function(err, result) {
					client.end();
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
			}
		});
  });
http.createServer(odataServer.handle.bind(odataServer)).listen(process.env.PORT ||1337);
//	}
//});
