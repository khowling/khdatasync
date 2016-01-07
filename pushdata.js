"use strict"

var Redis = require('ioredis'),
    redis = new Redis(process.env.REDIS_URL),
//    pg = require('pg'),  
//    client = new pg.Client(process.env.PG_URL),
    Connection = require('tedious').Connection,
    Request = require('tedious').Request,
    TYPES = require('tedious').TYPES,
    rest = require('restler'),
    async = require('./lib/async.js'),
    zlib = require('zlib');


// --------------------------------------------------------- Redis -> Salesforce
// Affinity Profiles
function* exportAffProfile(rkey, xref) {
    let custids = [];
    for (let i = 0; i < 10000; i++) {
      let rpop = yield redis.spop(rkey); //s
      if (rpop)
        custids.push(rpop);
      else 
        break;
    }
    console.log ('exportAffProfile # : ' + custids.length);
    let csvlines = [];
    for (let p of custids) {
      console.log ('exportAffProfile : getting ' + `profile:${p}:buckets`);
      if (xref.hasOwnProperty(p)) {
        let resObj = yield redis.hgetall (`profile:${p}:buckets`), resArray = [];
        Object.keys(resObj).forEach(prop => resArray.push (prop,  (Number.parseInt (resObj[prop]) || "0")/1000));
        csvlines.push(`${xref[p]},"${JSON.stringify(resArray).replace(/"/g, '\""')}"`);
      } else {
        console.log (`exportAffProfile : No SFDC refid for customer card ${p}`)
      }
    }
    return { popped: custids, formatted_out: csvlines};
}
// Slips
function* exportSlips(rkey) {
    let custids = [];
    for (let i = 0; i < 100; i++) {
      let rpop = yield redis.spop(rkey); //srandmember  spop
      if (rpop)
        custids.push(rpop);
      else break;
    }
    console.log ('exportSlips # : ' + custids.length);
    let strlines = [];
    for (let p of custids) {
//      console.log (`exportSlips : getting ${p}`);
      let comp64 = yield redis.hget (p, "slip");
//      console.log (`got :  ${comp64}`);
      strlines.push( JSON.parse(yield mkUnzipPromise(new Buffer(comp64, 'base64'))));
    }
    return { popped: custids, formatted_out: strlines};
}

function mkUnzipPromise(arg) {
  return new Promise ((resolve, reject) => {
    zlib.unzip(arg, function(err, buffer) {

      if (!err) {
        return resolve(buffer.toString());
      } else {
        return reject(err);
      }
    });
  });
}

// -------------------- BULK Postgres
var PGTABLES = {
  'slip':
    {schema: 'public', idsequence: 'slip_id_seq',table: 'slip',  additional: "tsdbinsert",
    fields:    ['companyid','customercardid','storeid','enddatetime','totalamount','tsreceived','tsqueueinsert','tsworkerreceived','tsstartcrunsh','tsendcrunsh'],
    sourceMap: ['Transaction.CompanyID','Transaction.Customer.CustomerID','Transaction.StoreID','Transaction.EndDateTime','Transaction.TotalAmount','tsreceived','tsqueueinsert','tsworkerreceived','tsstartcrunsh','tsendcrunsh']
  },
  'slipitem':
    {schema: 'public', idsequence: 'slipitem_id_seq',table: 'slipitem', itterator: 'Transaction.Sale', additional: "slip",
    fields:    ['codeinput', 'itemid', 'scaninput','quantity','currentunitprice','extendedamount','originalamount','attributevalue','sfdcitemid','dicountamount','discountableamount','discountname','promotionid'],
    sourceMap: ['CodeInput','ItemID','ScanInput','Quantity','CurrentUnitPrice','ExtendedAmount','OriginalAmount','AttributeValue','Item.sfid','Discount.DiscountAmount','Discount.DiscountableAmount','Discount.DiscountName','Discount.PromotionID']
  }
};
function resolvedot(strdot, val) {
  let sf = strdot.split('.'), ret = val[sf[0]];
  if (sf.length > 1)
    if (ret)
      ret =  ret[sf[1]];
    else
      ret =  null;
  if (sf.length > 2)
    if (ret)
      ret =  ret[sf[2]];
    else
      ret =  null;
  return ret;
}

function pgInsertSlips(pg, syncdef, slips, headids) {
  return new Promise ((resolve, reject) => {

    if (slips.length >0) {
      let insstr = `INSERT INTO ${syncdef.schema}.${syncdef.table} (id,${syncdef.additional},${syncdef.fields.join(',').toLowerCase()}) VALUES `,
          valpos = [], valarray = [];
      for (let hidx = 0; hidx < slips.length; hidx++) {
        for (let r of syncdef.itterator ? resolvedot(syncdef.itterator, slips[hidx]) : [slips[hidx]]) {
          let posrow = [];
          // id from sequence
          posrow.push (`nextval('${syncdef.schema}.${syncdef.idsequence}')`);
          if (syncdef.additional) {
            if (syncdef.additional === 'tsdbinsert')
              valarray.push (new Date(Date.now()));
            else if (syncdef.additional === 'slip')
              valarray.push (headids[hidx].id);
            posrow.push (`$${valarray.length}`);
          }

          for (let i = 0; i < syncdef.fields.length; i++) {
            let tf = syncdef.fields[i], sf = syncdef.sourceMap[i].split('.'),
                sval = resolvedot(syncdef.sourceMap[i], r);

            if (tf === "enddatetime" || tf === "tsreceived" || tf === "tsqueueinsert" || tf === "tsworkerreceived" || tf === "tsstartcrunsh" || tf === "tsendcrunsh")
              if (sval)
                valarray.push (new Date(sval));
              else
                valarray.push (null);
            else
              valarray.push (sval);
            posrow.push (`$${valarray.length}`);
          }
          valpos.push (`(${posrow.join(', ')})`);
        }
      }
      insstr+= valpos.join(',');
      insstr+= ' RETURNING id';
      // insert into pg
      pg.query({text: insstr, values: valarray}, function(err, result) {
        if(err) {
          console.error('error running query', err);
          reject (err);
        } else
          return resolve(result);
      });
    }
  });
}
// ------------------------------------------- AZURE SQL
var SQLTABLES = {
  'slip':
    {schema: 'dbo',table: 'AzureSlip',  additional: "tsdbinsert",
    fields:    ['companyid', 'customercardid','storeid','enddatetime','totalamount','tsreceived','tsqueueinsert','tsworkerreceived','tsstartcrunsh','tsendcrunsh'],
    sourceMap: ['Transaction.CompanyID','Transaction.Customer.CustomerID','Transaction.StoreID','Transaction.EndDateTime','Transaction.TotalAmount','tsreceived','tsqueueinsert','tsworkerreceived','tsstartcrunsh','tsendcrunsh']
  },
  'slipitem':
    {schema: 'dbo',table: 'AzureSlipItem', itterator: 'Transaction.Sale', additional: "slip",
    fields:    ['codeinput', 'itemid', 'scaninput','quantity','currentunitprice','extendedamount','originalamount','attributevalue','sfdcitemid','dicountamount','discountableamount','discountname','promotionid'],
    sourceMap: ['CodeInput','ItemID','ScanInput','Quantity','CurrentUnitPrice','ExtendedAmount','OriginalAmount','AttributeValue','Item.sfid','Discount.DiscountAmount','Discount.DiscountableAmount','Discount.DiscountName','Discount.PromotionID']
  }
};
function sqlInsertSlips(sql, syncdef, slips, headids) {
  return new Promise ((resolve, reject) => {

    if (slips.length >0) {
      let insstr = `INSERT INTO ${syncdef.schema}.${syncdef.table} (${syncdef.additional},${syncdef.fields.join(',').toLowerCase()}) OUTPUT INSERTED.id VALUES `,
          req_params = [], posall = [];
      for (let hidx = 0; hidx < slips.length; hidx++) {
        for (let r of syncdef.itterator ? resolvedot(syncdef.itterator, slips[hidx]) : [slips[hidx]]) {
          let posrow = [];

          if (syncdef.additional === 'tsdbinsert')
              posrow.push ('CURRENT_TIMESTAMP');
          else if (syncdef.additional === 'slip') {
              req_params.push([`P${req_params.length+1}`, TYPES.Int, headids[hidx].id]);
              posrow.push (`@P${req_params.length}`);
          }

          for (let i = 0; i < syncdef.fields.length; i++) {
            let tf = syncdef.fields[i], sf = syncdef.sourceMap[i].split('.'),
                sval = resolvedot(syncdef.sourceMap[i], r);

            if (tf === "enddatetime" || tf === "tsreceived" || tf === "tsqueueinsert" || tf === "tsworkerreceived" || tf === "tsstartcrunsh" || tf === "tsendcrunsh")
              if (sval)
                req_params.push ([`P${req_params.length+1}`, TYPES.DateTime, new Date(sval)]); 
              else
                req_params.push ([`P${req_params.length+1}`, TYPES.DateTime, null]);
            else if (tf == "totalamount")
              req_params.push ([`P${req_params.length+1}`, TYPES.Float, sval]);
            else
              req_params.push ([`P${req_params.length+1}`, TYPES.VarChar, sval]);
            posrow.push (`@P${req_params.length}`);
          }
          posall.push (`(${posrow.join(', ')})`);
        }
      }
      insstr+= posall.join(',') ;
      

      // insert into sql
      let retrow = [];
  //    console.log ('prepare ' + insstr);
      let request = new Request(insstr,  function(err) {
        if (err) {
          console.error('prepare insert error', err);
          reject ('prepare insert error' + err);
        } else {
          console.log('finished inserting ' + syncdef.table + ' : ' + retrow.length);
          return resolve(retrow);
        }
      });
      
      for (let p of req_params) {
        request.addParameter(p[0], p[1], p[2]);
      }

      request.on('row', function (columns) { 
        retrow.push({id: columns[0].value});
        //console.log ('row : ' + JSON.stringify(columns));
      });
      sql.execSql(request);
          
    } else 
      resolve([]);
  });
}
// -------------------- BULK SFDC
function sfdcUpdateAffProfileBulk(oauth, payloadCSV) {
  return new Promise ((resolve, reject) => {
    rest.post (oauth.instance_url + '/services/async/35.0/job', {
      headers: {
        'X-SFDC-Session': oauth.access_token,
        'Content-Type': 'application/xml'
      },
      data: '<?xml version="1.0" encoding="UTF-8"?>' +
            '<jobInfo xmlns="http://www.force.com/2009/06/asyncapi/dataload">' +
            '    <operation>update</operation>' +
            '    <object>AffinityProfile__c</object>' +
            '    <contentType>CSV</contentType>' +
            '</jobInfo>'
    }).on('complete', (jobresponse) => {

//      console.log ('sfdc bulk job');
      let jobid = jobresponse.jobInfo.id[0];
      //console.log ('jobid ' + jobid +  ', payload ' + payload);
      rest.post (oauth.instance_url + '/services/async/35.0/job/'+jobid+'/batch', {
        headers: {
          'X-SFDC-Session': oauth.access_token,
          'Content-Type': 'text/csv'
        },
        data : payloadCSV
      }).on('complete', (val) => {
//        console.log ('sfdc added batch');
        rest.post (oauth.instance_url + '/services/async/35.0/job/'+jobid, {
          headers: {
            'X-SFDC-Session': oauth.access_token,
            'Content-Type': 'application/xml'
          },
          data : '<?xml version="1.0" encoding="UTF-8"?>' +
                  '<jobInfo xmlns="http://www.force.com/2009/06/asyncapi/dataload">' +
                    '<state>Closed</state>' +
                  '</jobInfo>'
        }).on('complete', (val) => {
//          console.log ('sfdc close job ');
          resolve (val.jobInfo && `sfdc batch id : ${val.jobInfo.id[0]}`);
        });
      }).on('error', (err) => {
        console.error('error', err);
        reject('err : ' + err);
      }).on ('fail', (err) => {
        console.error('fail', err);
        reject('fail : ' + err);
      });
    }).on('error', (err) => {
      console.error('error', err);
      reject('err : ' + err);
    }).on ('fail', (err) => {
      console.error('fail', err);
      reject('fail : ' + err);
    });
  });
}

            
function exportSlipsMain(connection) {
  return new Promise ((resolve, reject) => {
    let rkey = "slips";
    console.log (`exportSlipsMain : look for ${rkey}`);

    async(exportSlips, rkey).then((redisprofiles) => {
      let popped = redisprofiles.popped, formatted_out =  redisprofiles.formatted_out;
      if (formatted_out.length >0) {
        console.error ('exportSlipsMain got Slips : ' + formatted_out.length);
        sqlInsertSlips (connection, SQLTABLES.slip, formatted_out).then(succ => {
          console.log (`exportSlipsMain inserted ${SQLTABLES.slip.table} : ${succ.length}`);
          sqlInsertSlips (connection, SQLTABLES.slipitem, formatted_out, succ).then(succ => {
            console.log (`exportSlipsMain inserted ${SQLTABLES.slipitem.table} : ${succ.length}`);
            redis.del(popped, () => {
              resolve (succ);
            });
          }, rej => {
            reject ('sqlInsertSlipsItems rejection : ' + rej);
          }).catch (err => {
            reject ('sqlInsertSlipsItems error ' + err);
          });
        }, rej => {
          reject ('sqlInsertSlips rejection : ' + rej);
        }).catch (err => {
          reject ('sqlInsertSlips error ' + err);
        });
      } else
        return  resolve ('Nothing to do');
    }, rej => {
      reject ('exportSlips rejection : ' + rej);
    }).catch (err => {
      reject ('exportSlips error ' + err);
    });
  });
}

function exportPromotionsMain (oauthres, connection) {
  return new Promise ((resolve, reject) => {
    let rkey = "promotions";

    redis.hgetall ("cardtoaffid", (err, xref) => {
      if (err) {
        console.error ('cardtoaffid err : ' + err);
        redis.disconnect();
        connection.close();
      } else {
        console.log (`exportPromotionsMain : Look for ${rkey}`);
        async(exportAffProfile, rkey, xref).then((redisprofiles) => {
          let popped = redisprofiles.popped, formatted_out =  redisprofiles.formatted_out;
          if (formatted_out.length >0) {
            console.log ('Updating Affinity Profiles # ' +formatted_out.length);
            let payloadCSV = "Id,Transfer__c\n" + formatted_out.join('\n')
//                      console.log ('payloadCSV: ' + payloadCSV);
            sfdcUpdateAffProfileBulk (oauthres, payloadCSV).then(succ => {
              resolve (succ);
            }, err => {
              console.error ('sfdcUpdateAffProfileBulk error, put back popped : ', err);
              redis.sadd(rkey, popped, () => reject (err));
            });
          } else {
            resolve ('Nothing to do');
          }
        }, rej => {
          reject ('exportAffProfile rejection : ' + rej);
        }).catch (err => {
          reject ('exportAffProfile error ' + err);
        });
      }
    });
  });
}
            

console.log (`Connecting Redis ......`);
redis.on('connect',  () => {
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
      console.log ('Connected Salesforce ......');  
    	rest.post('https://login.salesforce.com/services/oauth2/token', {
    		query: {
    			grant_type:'password',
    			client_id:'3MVG9Rd3qC6oMalUd.EEm8FrmpaPkQs.Jb6CpcCMWu4CKLSmevbJsPy5EALngHRwoS13Zlv37VyvuHMVwScZD',
    			client_secret:'2727761931602693303',
    			username: process.env.SF_USERNAME,
    			password: process.env.SF_PASSWORD
    		}}).on('complete', function(oauthres) {

      		if (oauthres.access_token && oauthres.instance_url) {
            exportSlipsMain(connection).then (succ => {
              exportPromotionsMain(oauthres, connection).then (succ => {
                console.log (`exportPromotionsMain success :  ${JSON.stringify(succ)}`);
                redis.disconnect();
                connection.close();
              }, err => {
                console.error (`exportPromotionsMain failed :  ${JSON.stringify(err)}`);
                redis.disconnect();
                connection.close();
              });
            }, err => {
              console.error (`exportSlipsMain failed :  ${JSON.stringify(err)}`);
              redis.disconnect();
              connection.close();
            });
          } else {
            console.error('no salesforce');
            redis.disconnect();
            connection.close();
          }
        });
    }
  });
});

redis.on('error', function (e) {
  console.error ('Redis error',e);
});
