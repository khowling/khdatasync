'use strict'

var http = require('http'),
    ODataServer = require("simple-odata-server"),
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
					  "tsdbinsert": {"type": "Edm.DateTimeOffset"},
            "tsdbinsertfmt": {"type": "Edm.String"},
            "tsendcrunshfmt": {"type": "Edm.String"},
            "tsstartcrunshfmt": {"type": "Edm.String"},
            "tsworkerreceivedfmt": {"type": "Edm.String"},
            "tsqueueinsertfmt": {"type": "Edm.String"},
            "tsreceivedfmt": {"type": "Edm.String"},
            "enddatetimefmt": {"type": "Edm.String"},
            "tsreceivedtoinsert": {"type": "Edm.String"}
				},
        "SlipItemType": {
            "id": {"type": "Edm.String", key: true},
            "slip": {"type": "Edm.String"},
            "codeinput": {"type": "Edm.String"},
            "scaninput": {"type": "Edm.String"},
            "quantity": {"type": "Edm.Double"},
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


console.log ('Connected Azure SQL ......');
let connection = new Connection({
    userName: process.env.SQL_USERNAME,
    password: process.env.SQL_PASSWORD,
    server: process.env.SQL_HOSTNAME,
    // When you connect to Azure SQL Database, you need these next options.
    options: {encrypt: true, database: process.env.SQL_DBNAME}
});
connection.on('err', (err) => {
  console.error('connect to SQL err', err);
});
connection.on('connect', (err) => {
  if(err) {
    return console.error('could not connect to SQL', err);
  } else {

    console.log ('Starting odata server on : ' + process.env.ODATA_HOSTNAME);
    var odataServer = ODataServer(process.env.ODATA_HOSTNAME)
      .model(model)
      .query((setName, query, cb) => {

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
            let enttype = model.entitySets[query['collection']].entityType.split(".")[1];
            if (model.entityTypes[enttype][c.metadata.colName]["type"] === "Edm.Double" && c.value && c.value % 1 == 0) {
              console.log ('times by 1.0001: ' + c.value);
              row[c.metadata.colName] = c.value + 0.00001;
            } else
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
      http.createServer(odataServer.handle.bind(odataServer)).listen(process.env.PORT ||1337);
    }
});
