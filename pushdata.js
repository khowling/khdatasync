"use strict"

var Redis = require('ioredis'),
    redis = new Redis(process.env.REDIS_URL),
//    pg = require('pg'),
//    client = new pg.Client(process.env.PG_URL),
    Connection = require('tedious').Connection,
    Request = require('tedious').Request,
    TYPES = require('tedious').TYPES,
    uuid = require('node-uuid'),
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
//      console.log ('exportAffProfile : getting ' + `profile:${p}:buckets`);
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
    for (let i = 0; i < 10000; i++) {
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
// Coupons
function* exportCoupons(rkey) {
    let custids = [];
    for (let i = 0; i < 10000; i++) {
      let rpop = yield redis.spop(rkey); //s
      if (rpop)
        custids.push(rpop);
      else
        break;
    }
    console.log ('exportCoupons # : ' + custids.length);
    let csvlines = [];
    for (let p of custids) {
      let resObj = yield redis.hgetall (p), resArray = [];
      console.log (`exportCoupons ${p}: ${JSON.stringify(resObj)}`);
      if (parseInt(resObj.Eingeloest_Am__c) > 0) {
        csvlines.push(`${resObj.Id},${new Date(parseInt(resObj.Eingeloest_Am__c)).toISOString()}`);
      }
    }
    return { popped: custids, formatted_out: csvlines};
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
    {schema: 'dbo',table: 'AzureSlip',  head: true, additional: "tsdbinsert",
    fields:    ['companyid', 'customercardid','storeid','enddatetime','totalamount','tsreceived','tsqueueinsert','tsworkerreceived','tsstartcrunsh','tsendcrunsh'],
    sourceMap: ['Transaction.CompanyID','Transaction.Customer.CustomerID','Transaction.StoreID','Transaction.EndDateTime','Transaction.TotalAmount','tsreceived','tsqueueinsert','tsworkerreceived','tsstartcrunsh','tsendcrunsh']
  },
  'slipitem':
    {schema: 'dbo',table: 'AzureSlipItem', head: false, itterator: 'Transaction.Sale', additional: "slip",
    fields:    ['codeinput', 'itemid', 'scaninput','quantity','currentunitprice','extendedamount','originalamount','attributevalue','sfdcitemid','dicountamount','discountableamount','discountname','promotionid'],
    sourceMap: ['CodeInput','ItemID','ScanInput','Quantity','CurrentUnitPrice','ExtendedAmount','OriginalAmount','AttributeValue','Item.sfid','Discount.DiscountAmount','Discount.DiscountableAmount','Discount.DiscountName','Discount.PromotionID']
  }
};
/*
var MAX_SLIP_INSERT = 10; //(10 = 100 slip items = 2000 slipitem positional fields (SQL max 2100))
function sqlInsertSlips(sql, syncdef, slips, headids, sidx, retrow) {
  return new Promise ((resolve, reject) => {
    if (!sidx) {
      sidx = 0; retrow = [];
    }
    if (slips.length > sidx) {
      let insstr = `INSERT INTO ${syncdef.schema}.${syncdef.table} (${syncdef.additional},${syncdef.fields.join(',').toLowerCase()}) OUTPUT INSERTED.id VALUES `,
          req_params = [], posall = [];
      for (let hidx = sidx; hidx < slips.length && hidx < (sidx + MAX_SLIP_INSERT); hidx++) {
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
            else if (tf == 'totalamount' || tf ==='quantity' || tf === 'currentunitprice' || tf === 'extendedamount' || tf === 'originalamount' || tf === 'dicountamount' || tf === 'discountableamount')
              req_params.push ([`P${req_params.length+1}`, TYPES.Float, sval]);
            else
              req_params.push ([`P${req_params.length+1}`, TYPES.VarChar, sval]);
            posrow.push (`@P${req_params.length}`);
          }
          posall.push (`(${posrow.join(', ')})`);
        }
      }
      insstr+= posall.join(',') ;


  //    console.log ('prepare ' + insstr);
      let request = new Request(insstr,  function(err) {
        if (err) {
          console.error('prepare insert error', err);
          reject ('prepare insert error' + err);
        } else {
          console.log('finished inserting ' + syncdef.table + ' : ' + retrow.length);
          if (slips.length > (sidx + MAX_SLIP_INSERT))
            sqlInsertSlips(sql, syncdef, slips, headids, (sidx + MAX_SLIP_INSERT), retrow).then(resolve);
          else {
            return resolve(retrow);
          }
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
*/
function sqlInsertSlipsBulk(sql, syncdef, slips, headids) {
  return new Promise ((resolve, reject) => {
    let uuid_hidx = syncdef.head && new Map();

    if (slips.length >0) {
      var bulkLoad = sql.newBulkLoad(`${syncdef.schema}.${syncdef.table}`, function (error, rowCount) {
        if (error) {

          reject ('Error preparing bulkload : ' + error);
        } else {
          console.log('inserted %d rows', rowCount);
          if (syncdef.head) {
            let slipsidx_recid = new Map(),
                inclause = "'" + Array.from(uuid_hidx.keys()).join("','") + "'",
                squery = `SELECT uuid, id FROM ${syncdef.schema}.${syncdef.table} WHERE uuid IN (${inclause})`;
          //  console.log (squery);
            let qrequest = new Request(squery, function(err) {
                  if (err) {
                    console.log('select error : ' + err);
                    reject(err);
                  } else {
                    resolve (slipsidx_recid);
                  }
                });

            qrequest.on('row', function (columns) {
              //console.log (`columns: ${JSON.stringify(columns)}`);
              slipsidx_recid.set(uuid_hidx.get(columns[0].value), columns[1].value);
              //console.log (`(${columns[0].value}) set ${uuid_hidx.get(columns[0].value)} :  ${columns[1].value}`);
            });
            sql.execSql(qrequest);
          } else
            resolve (rowCount);
        }
      });
      // Add UniqueId or Forign Key Column Names
      if (syncdef.head) {
          bulkLoad.addColumn('uuid', TYPES.UniqueIdentifierN, { nullable: true });
          bulkLoad.addColumn('tsdbinsert', TYPES.DateTime, { nullable: true });
      } else {
          bulkLoad.addColumn('slip', TYPES.Int, { nullable: true });
      }
      // Add rest of Column names
      for (let tf of syncdef.fields) {
        if (tf === "enddatetime" || tf === "tsreceived" || tf === "tsqueueinsert" || tf === "tsworkerreceived" || tf === "tsstartcrunsh" || tf === "tsendcrunsh")
          bulkLoad.addColumn(tf, TYPES.DateTime, { nullable: true });
          else if (tf == 'totalamount' || tf ==='quantity' || tf === 'currentunitprice' || tf === 'extendedamount' || tf === 'originalamount' || tf === 'dicountamount' || tf === 'discountableamount')
          bulkLoad.addColumn(tf, TYPES.Float, { nullable: true });
        else
          bulkLoad.addColumn(tf, TYPES.VarChar, { nullable: true });
      }

      // Add Row Data
      for (let hidx = 0; hidx < slips.length; hidx++) {
        for (let r of syncdef.itterator ? resolvedot(syncdef.itterator, slips[hidx]) : [slips[hidx]]) {
          let posrow = [];
            // Add UniqueId or Forign Key Data
          if (syncdef.head) {
            let uid = uuid.v4().toUpperCase();
            uuid_hidx.set(uid, hidx);
            posrow.push (uid);
            posrow.push (new Date());
          } else
            posrow.push (headids.get(hidx));

          for (let i = 0; i < syncdef.fields.length; i++) {
            let tf = syncdef.fields[i], sf = syncdef.sourceMap[i].split('.'),
                sval = resolvedot(syncdef.sourceMap[i], r);

            if (tf === "enddatetime" || tf === "tsreceived" || tf === "tsqueueinsert" || tf === "tsworkerreceived" || tf === "tsstartcrunsh" || tf === "tsendcrunsh")
              if (sval)
                posrow.push (new Date(sval));
              else
                posrow.push (null);
            else
              posrow.push (sval);
          }
        //  console.log ('adding row : ' + JSON.stringify(posrow));
          bulkLoad.addRow(posrow);
        }
      }
      sql.execBulkLoad(bulkLoad);
    } else
      resolve([]);
  });
}
// -------------------- BULK SFDC
function sfdcUpdateAffProfileBulk(oauth, sobject, payloadCSV) {
  return new Promise ((resolve, reject) => {
    rest.post (oauth.instance_url + '/services/async/35.0/job', {
      headers: {
        'X-SFDC-Session': oauth.access_token,
        'Content-Type': 'application/xml'
      },
      data: '<?xml version="1.0" encoding="UTF-8"?>' +
            '<jobInfo xmlns="http://www.force.com/2009/06/asyncapi/dataload">' +
            '    <operation>update</operation>' +
            '    <object>'+ sobject +'</object>' +
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

    async(exportSlips, rkey).then((redisSlips) => {
      let popped = redisSlips.popped, formatted_out =  redisSlips.formatted_out;
      if (formatted_out.length >0) {
        console.log ('exportSlipsMain got Slips : ' + formatted_out.length);
        sqlInsertSlipsBulk (connection, SQLTABLES.slip, formatted_out).then(headmap => {
          console.log (`exportSlipsMain inserted ${SQLTABLES.slip.table} : ${headmap.size}`);
          sqlInsertSlipsBulk (connection, SQLTABLES.slipitem, formatted_out, headmap).then(linescnt => {
            console.log (`exportSlipsMain inserted ${SQLTABLES.slipitem.table} : ${linescnt}`);
            redis.del(popped, () => {
              resolve ("Done");
            });
          }, rej => {
            redis.sadd(rkey, popped); // put back popped
            reject ('sqlInsertSlipsBulk rejection : ' + rej);
          }).catch (err => {
            redis.sadd(rkey, popped); // put back popped
            reject ('sqlInsertSlipsBulk error ' + err);
          });
        }, rej => {
          redis.sadd(rkey, popped); // put back popped
          reject ('sqlInsertSlipsBulk rejection : ' + rej);
        }).catch (err => {
          redis.sadd(rkey, popped); // put back popped
          reject ('sqlInsertSlipsBulk error ' + err);
        });
      } else
        return  resolve ('Nothing to do');
    }, rej => {
      reject ('exportSlipsMain rejection : ' + rej);
    }).catch (err => {
      reject ('exportSlipsMain error ' + err);
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
            sfdcUpdateAffProfileBulk (oauthres, 'AffinityProfile__c', payloadCSV).then(succ => {
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


function exportCouponsMain (oauthres, connection) {
  return new Promise ((resolve, reject) => {
    let rkey = "redeems";
    console.log (`exportCouponsMain : Look for ${rkey}`);
    async(exportCoupons, rkey).then((rediscoupons) => {
      let popped = rediscoupons.popped, formatted_out =  rediscoupons.formatted_out;
      if (formatted_out.length >0) {
        console.log ('Updating Coupons  # ' +formatted_out.length);
        let payloadCSV = "Id,Eingeloest_Am__c\n" + formatted_out.join('\n')
    //                      console.log ('payloadCSV: ' + payloadCSV);
        sfdcUpdateAffProfileBulk (oauthres, 'Coupon_Zuweisung__c', payloadCSV).then(succ => {
          resolve (succ);
        }, err => {
          console.error ('exportCouponsMain error, put back popped : ', err);
          redis.sadd(rkey, popped, () => reject (err));
        });
      } else {
        resolve ('Nothing to do');
      }
    }, rej => {
      reject ('exportCoupons rejection : ' + rej);
    }).catch (err => {
      reject ('exportCoupons error ' + err);
    });
  });
}

// -------------------- MAIN
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
              console.log (`exportSlipsMain success :  ${JSON.stringify(succ)}`);
              exportPromotionsMain(oauthres, connection).then (succ => {
                console.log (`exportPromotionsMain success :  ${JSON.stringify(succ)}`);
                exportCouponsMain (oauthres, connection).then (succ => {
                  console.log (`exportCouponsMain success :  ${JSON.stringify(succ)}`);
                  redis.disconnect();
                  connection.close();
                }, err => {
                  console.error (`exportCouponsMain failed :  ${JSON.stringify(err)}`);
                  redis.disconnect();
                  connection.close();
                  process.exit(1);
                });
              }, err => {
                console.error (`exportPromotionsMain failed :  ${JSON.stringify(err)}`);
                redis.disconnect();
                connection.close();
                process.exit(1);
              });
            }, err => {
              console.error (`exportSlipsMain failed :  ${JSON.stringify(err)}`);
              redis.disconnect();
              connection.close();
              process.exit(1);
            });
          } else {
            console.error('no salesforce');
            redis.disconnect();
            connection.close();
            process.exit(1);
          }
        });
    }
  });
});

redis.on('error', function (e) {
  console.error ('Redis error',e);
  process.exit(1);
});
