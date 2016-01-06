"use strict"

var Redis = require('ioredis'),
    redis = new Redis(process.env.REDIS_URL),
    pg = require('pg'),
    client = new pg.Client(process.env.PG_URL),
    rest = require('restler'),
    async = require('./async.js'),
    zlib = require('zlib');


// --------------------------------------------------------- Redis -> Salesforce
// Affinity Profiles
function* exportAffProfile(oauth, rkey, xref) {
    let custids = [];
    for (let i = 0; i < 1000; i++) {
      let rpop = yield redis.spop(rkey); //s
      if (rpop)
        custids.push(rpop);
      else break;
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
function* exportSlips(oauth, rkey) {
    let custids = [];
    for (let i = 0; i < 2; i++) {
      let rpop = yield redis.srandmember(rkey); //srandmember  spop
      if (rpop)
        custids.push(rpop);
      else break;
    }
    console.log ('exportSlips # : ' + custids.length);
    let csvlines = [];
    for (let p of custids) {
//      console.log (`exportSlips : getting ${p}`);
      let comp64 = yield redis.hget (p, "slip");
//      console.log (`got :  ${comp64}`);
      csvlines.push( JSON.parse(yield mkUnzipPromise(new Buffer(comp64, 'base64'))));
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
          resolve (val);
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


console.log (`Connecting Redis ...... [${process.env.REDIS_URL}]`);
redis.on('connect', function () {
  client.connect((err) => {
    if(err) {
      return console.error('could not connect to postgres', err);
    } else {
    	rest.post('https://login.salesforce.com/services/oauth2/token', {
    		query: {
    			grant_type:'password',
    			client_id:'3MVG9Rd3qC6oMalUd.EEm8FrmpaPkQs.Jb6CpcCMWu4CKLSmevbJsPy5EALngHRwoS13Zlv37VyvuHMVwScZD',
    			client_secret:'2727761931602693303',
    			username: process.env.SF_USERNAME,
    			password: process.env.SF_PASSWORD
    		}}).on('complete', function(oauthres) {

      		if (oauthres.access_token && oauthres.instance_url) {

            if (false) {
              let rkey = "slips";
              console.log ('Look for ${rkey}');

              async(exportSlips, oauthres, rkey).then((redisprofiles) => {
                let popped = redisprofiles.popped, formatted_out =  redisprofiles.formatted_out;
                if (formatted_out.length >0) {
                  console.error ('exportSlips got SLips : ' + formatted_out.length);
                  pgInsertSlips (client, PGTABLES.slip, formatted_out).then(succ => {
                    console.log (`got ${JSON.stringify(succ)}`);

                    pgInsertSlips (client, PGTABLES.slipitem, formatted_out, succ.rows).then(succ => {
                      if (false) // debug
                        redis.del(popped, () => {
                          console.log (`got ${JSON.stringify(succ)}`);
                          redis.disconnect();
                          client.end();
                        });  // ### DEBUG LINE ONLY
                      else {
                        console.log (`got ${JSON.stringify(succ)}`);
                        redis.disconnect();
                        client.end();
                      }
                    });


                  }, rej => {
                    console.error ('pgInsertSlips rejection : ' + rej);
                    redis.disconnect();
                    client.end();
                  }).catch (err => {
                    console.error ('pgInsertSlips error ' + err);
                    redis.disconnect();
                    client.end();
                  });

                }
              }, rej => {
                console.error ('exportSlips rejection : ' + rej);
                redis.disconnect();
                client.end();
              }).catch (err => {
                console.error ('exportSlips error ' + err);
                redis.disconnect();
                client.end();
              });
            }

            if (false) {
              let rkey = "promotions";
              console.log ('Look for ${rkey}');
              redis.hgetall ("cardtoaffid", (err, xref) => {
                if (err) {
                  console.error ('cardtoaffid err : ' + err);
                  redis.disconnect();
                  client.end();
                } else {

                  async(exportAffProfile, oauthres, rkey, xref).then((redisprofiles) => {
                    let popped = redisprofiles.popped, formatted_out =  redisprofiles.formatted_out;
                    if (formatted_out.length >0) {
                      console.log ('Updating Affinity Profiles # ' +formatted_out.length);
                      let payloadCSV = "Id,Transfer__c\n" + formatted_out.join('\n')
//                      console.log ('payloadCSV: ' + payloadCSV);
                      sfdcUpdateAffProfileBulk (oauthres, payloadCSV).then(succ => {
                        console.log ('completed '+ JSON.stringify(succ));
                        if (false) // debug
                          redis.sadd(rkey, popped, () => {
                            redis.disconnect();
                            client.end();
                          });  // ### DEBUG LINE ONLY
                        else {
                          redis.disconnect();
                          client.end();
                        }
                      }, err => {
                        console.error ('sfdcUpdateAffProfileBulk error', err);
                        console.log ('put back popped for next run');
                        redis.sadd(rkey, popped);
                        redis.disconnect();
                        client.end();
                      });
                    } else {
                      console.log ('Nothing to do');
                      redis.disconnect();
                      client.end();
                    }
                  }, rej => {
                    console.error ('exportAffProfile rejection : ' + rej);
                    redis.disconnect();
                    client.end();
                  }).catch (err => {
                    console.error ('exportAffProfile error ' + err);
                    redis.disconnect();
                    client.end();
                  });
                }
              });
            }
          } else {
            console.error('no salesforce');
            redis.disconnect();
            client.end();
          }
        });
    }
  });
});

redis.on('error', function (e) {
  console.error ('Redis error',e);
  client.end();
});
