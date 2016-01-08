'use strict'

var http = require('http'),
    ODataServer = require("simple-odata-server"),
    //pg = require('pg');
    Connection = require('tedious').Connection,
    Request = require('tedious').Request,
    TYPES = require('tedious').TYPES;

var model = {
    namespace: "jsreport",
    entityTypes: {
        "SlipType": {
            "id": {"type": "Edm.String", key: true},
            "customercardid": {"type": "Edm.String"},
            "companyid": {"type": "Edm.String"},
            "storeid": {"type": "Edm.String"},
            "totalamount": {"type": "Edm.Double"},
            "enddatetime": {"type": "Edm.DateTimeOffset"},  //yyyy-MM-ddTHH:mm:ss.fffZ or yyyy-MM-ddTHH:mm:ss.fff[+&#124;-]HH:mm 2015-04-15T10:30:09.7550000Z
						"tsreceived": {"type": "Edm.DateTimeOffset"},
					  "tsqueueinsert": {"type": "Edm.DateTimeOffset"},
					  "tsworkerreceived": {"type": "Edm.DateTimeOffset"},
					  "tsstartcrunsh": {"type": "Edm.DateTimeOffset"},
					  "tsendcrunsh": {"type": "Edm.DateTimeOffset"},
					  "tsdbinsert": {"type": "Edm.DateTimeOffset"}
				},
        "SlipItemType": {
            "id": {"type": "Edm.String", key: true},
            "slip": {"type": "Edm.String"},
            "codeinput": {"type": "Edm.String"},
            "scaninput": {"type": "Edm.String"},
            "quantity": {"type": "Edm.Decimal"},
            "currentunitprice": {"type": "Edm.Double"},
            "extendedamount": {"type": "Edm.Double"},
            "originalamount": {"type": "Edm.Double"},  //yyyy-MM-ddTHH:mm:ss.fffZ or yyyy-MM-ddTHH:mm:ss.fff[+&#124;-]HH:mm 2015-04-15T10:30:09.7550000Z
						"itemid": {"type": "Edm.String"},
					  "sfdcitemid": {"type": "Edm.String"},
					  "attributevalue": {"type": "Edm.String"},
					  "dicountamount": {"type": "Edm.Double"},
					  "discountableamount": {"type": "Edm.Double"},
					  "discountname": {"type": "Edm.String"},
            "promotionid": {"type": "Edm.String"}
				}
    },
    entitySets: {
        "AzureSlip": {
          entityType: "jsreport.SlipType"
        },
        "AzureSlipItem": {
          entityType: "jsreport.SlipItemType"
        }
    }
};


//pg.defaults.poolSize = 5;
//pg.defaults.poolIdleTimeout = 3000; // 5 seconds (try to overcome the azure loadbalancer)
//pg.connect(process.env.PG_URL, function(err, client, done) {
//  if(err) {
//    return console.error('could not connect to postgres', err);
//  } else {

console.log ('Connected Azure SQL ......');
let connection = new Connection({
    userName: 'khowling',
    password: 'sForce123',
    server: 'affinitydb.database.windows.net',
    // When you connect to Azure SQL Database, you need these next options.
    options: {encrypt: true, database: 'affinitydb'}
});
connection.on('connect', (err) => {
  if(err) {
    return console.error('could not connect to SQL', err);
  } else {

    console.log ('Starting odata server on : ' + process.env.ODATA_HOSTNAME);
    var odataServer = ODataServer(process.env.ODATA_HOSTNAME)
      .model(model)
      .query((setName, query, cb) => {
//		let client = new pg.Client(process.env.PG_URL);
//		console.log ('Connecting to (need to do it here because Azure cannot keep open a connection) : ' + process.env.PG_URL);

//		client.connect(function(err) {
//		  if(err) {
//		    console.error('could not connect to postgres', err);
//				cb(err);
//		  } else {
				let qstr = ` ${Object.keys(query['$select']).join(',')} FROM  ${query['collection']}`;
				if (query['$filter'] && Object.keys(query['$filter']).length >0) {

					let calWhere = function (whobj) {
						let fkys = Object.keys(whobj);
						if (fkys.length >0) {
							if (fkys[0] === "$or" || fkys[0] === "$and") {
								let ors = [], orvals = whobj[fkys[0]];
								for (let f of orvals) {
									if (Object.keys(f)[0] === "$or" || Object.keys(f)[0] === "$and")
										ors.push( calWhere (f));
									else
										ors.push(` ${Object.keys(f)[0]} = '${f[Object.keys(f)[0]]}' `);
								}
								return `  ${ors.join(fkys[0].substring(1))} `;
							} else
								return ` ${fkys[0]} = '${whobj[fkys[0]]}' `;
						}
					}
					qstr+= ' WHERE ' + calWhere (query['$filter']);
				}
				if (query['$limit']) {
					//qstr+= " limit " + query['$limit'];
          qstr = `TOP (${query['$limit']}) ` + qstr;
				} else if (query['$top']) {
          qstr = `TOP (${query['$top']}) ` + qstr;
        }

				console.log ("request with : " + JSON.stringify(query));
				console.log ("query with : " + qstr);

        var result = [];
        let request = new Request('SELECT ' + qstr, function(err) {
          if (err) {
            console.log ("query err " + JSON.stringify(err));
            cb(err);
          } else {
            console.log ("query succ " + JSON.stringify(result));
						cb(null, {
								count: result.length,
								value: result
						});
          }
        });

        request.on('row', function(columns) {
          console.log ("query row " + JSON.stringify(columns));
          let row = {};
          for (let c of columns) {
            row[c.metadata.colName] = c.value;
          }
          console.log ("push row " + JSON.stringify(row));
          result.push(row);
        });

        request.on('done', function(rowCount, more) {
          console.log(rowCount + ' rows returned');
        });
        connection.execSql(request);
      });
/*
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
*/
      http.createServer(odataServer.handle.bind(odataServer)).listen(process.env.PORT ||1337);
    }
});
//	}
//});
