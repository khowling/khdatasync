"use strict"

var Redis = require('ioredis'),
    redis = new Redis(process.env.REDIS_URL),
//    pg = require('pg'),
//    client = new pg.Client(process.env.PG_URL),
    rest = require('restler'),
    async = require('./lib/async.js');



// ------------------------------------------------------------- Salesforce -> Redis
// ----------------- AffinityProfiles
let importAffProfile = function(oauth, nextRecordsUrl) {
	return new Promise(function (resolve, reject)  {
		// query for ALL salesforce records
		rest.get (oauth.instance_url + ( nextRecordsUrl || '/services/data/v35.0/query'), {
			headers: {
				'Authorization': 'Bearer ' +oauth.access_token,
				'Sforce-Query-Options': 'batchSize=500'
			},
			query: {
				q:'select Id, CustomerCardID__c, Transfer__c  from AffinityProfile__c'
			}
		}).on('complete', (sfrecs) => {
//			console.log ('val : ' + JSON.stringify(val));
			if (sfrecs.records && sfrecs.records.length >0) {
        let setxref = []
        for (let sfrec of sfrecs.records) {
          setxref.push(sfrec.CustomerCardID__c);
          setxref.push(sfrec.Id);
        }
        redis.hmset ("cardtoaffid",  setxref).then(succ => {
          async(updateRedisWithSFDCAffProfiles, sfrecs.records).then(succ => {
            console.log ('Affinity Profiles added: ' + succ);
            if (sfrecs.done)
              resolve({});
            else
              importAffProfile (oauth, sfrecs.nextRecordsUrl).then(resolve);
          }, err => {
            console.error('error reject', err);
            reject(err);
          }).catch(err => {
            console.error('error catch', err);
            reject(err);
          });
        });
      } else {
				console.error('sfrec', JSON.stringify(sfrecs));
				resolve ({});
			}
		}).on('error', (err) => {
			console.error('error', err);
			reject('err : ' + err);
		}).on ('fail', (err) => {
			console.error('fail', err);
			reject('fail : ' + err);
		});
	}).catch ((e) => console.error ('catch ', e));
}

function* updateRedisWithSFDCAffProfiles (sfrecs) {
  let added = 0;
  for (let sfrec of sfrecs) {
    let key = `profile:${sfrec.CustomerCardID__c}:buckets`,
        exists = yield redis.exists(key);
    if (exists == 0) {
      added++;
      yield redis.hmset (key, JSON.parse(sfrec.Transfer__c).map((v,i) => i%2? Number.isFinite(v) && v*1000 || 0 : v));
    }
  }
  return added;
}

// ----------------- Items
let importItems = function(oauth, nextRecordsUrl) {
	return new Promise(function (resolve, reject)  {
		// query for ALL salesforce records
		rest.get (oauth.instance_url + ( nextRecordsUrl || '/services/data/v35.0/query'), {
			headers: {
				'Authorization': 'Bearer ' +oauth.access_token,
				'Sforce-Query-Options': 'batchSize=500'
			},
			query: {
				q:'select Id, ItemNmb__c, Store__c, WGI__c, ItemFamily__c, SubItemGroup__c from Item__c'
			}
		}).on('complete', (sfrecs) => {
//			console.log ('val : ' + JSON.stringify(val));
			if (sfrecs.records && sfrecs.records.length >0) {

          async(updateRedisWithSFDCItems, sfrecs.records).then(succ => {
            console.log ('Items added: ' + succ);
            if (sfrecs.done)
              resolve({});
            else
              importItems (oauth, sfrecs.nextRecordsUrl).then(resolve);
          }, err => {
            console.error('error reject', err);
            reject(err);
          }).catch(err => {
            console.error('error catch', err);
            reject(err);
          });

      } else {
				console.error('sfrec', JSON.stringify(sfrecs));
				resolve ({});
			}
		}).on('error', (err) => {
			console.error('error', err);
			reject('err : ' + err);
		}).on ('fail', (err) => {
			console.error('fail', err);
			reject('fail : ' + err);
		});
	}).catch ((e) => console.error ('catch ', e));
}

function* updateRedisWithSFDCItems (sfrecs) {
  let added = 0;
  for (let sfrec of sfrecs) {
    let key = `item:${Number.parseInt(sfrec.ItemNmb__c)}`;
    added++;
    yield redis.hmset (key, ["itemnmb__c", Number.parseInt(sfrec.ItemNmb__c) ,"store__c" , sfrec.Store__c ,"sfid" , sfrec.Id, "wgi__c" , sfrec.WGI__c , "subitemgroup__c", sfrec.SubItemGroup__c , "itemfamily__c", sfrec.ItemFamily__c]);
  }
  return added;
}
// ----------------- AffinityRules
let importAffRules = function(oauth, nextRecordsUrl) {
	return new Promise(function (resolve, reject)  {
		// query for ALL salesforce records
		rest.get (oauth.instance_url + ( nextRecordsUrl || '/services/data/v35.0/query'), {
			headers: {
				'Authorization': 'Bearer ' +oauth.access_token,
				'Sforce-Query-Options': 'batchSize=500'
			},
			query: {
				q:'select Id, Active__c, GeneratedCode__c from AffinityRule__c'
			}
		}).on('complete', (sfrecs) => {
//			console.log ('val : ' + JSON.stringify(val));
			if (sfrecs.records && sfrecs.records.length >0) {

        let addRules = [], rmRules = [];
        for (let sfrec of sfrecs.records) {
          if (sfrec.Active__c && sfrec.GeneratedCode__c) {
            addRules.push (sfrec.Id);
            addRules.push (sfrec.GeneratedCode__c.replace(/(?:\r\n|\r|\n|\t)/g, ' '));
          } else {
            rmRules.push (sfrec.Id);
          }
        }

        let p = null, succ = (succ) => {
              console.log (`AffinityRule processed : ${succ}`)
              if (sfrecs.done)
                resolve(succ)
              else
                importAffRules (oauth, sfrecs.nextRecordsUrl).then(resolve)
            }, err = (err) => { reject (err) };

        if (addRules.length > 0) {
          console.log (`AffinityRule adding: ${addRules.length/2}`);
          p = redis.hmset ("affinityrules", addRules);
        }
        if (rmRules.length > 0) {
          console.log (`AffinityRule removing: ${rmRules.length}`);
          let addfn = (succ) => {
                console.log (`AffinityRule processed: ${succ}`);
                return redis.hdel ("affinityrules", rmRules);
              };

          if (!p)
            p = addfn();
          else
            p = p.then(addfn, err);
        }

        if (p)
          p.then(succ, err);
        else {
          succ(0);
        }

      } else {
				console.error('sfrec', JSON.stringify(sfrecs));
				resolve ({});
			}
		}).on('error', (err) => {
			console.error('error', err);
			reject('err : ' + err);
		}).on ('fail', (err) => {
			console.error('fail', err);
			reject('fail : ' + err);
		});
	}).catch ((e) => console.error ('catch ', e));
}


console.log (`Connecting Redis ...... [${process.env.REDIS_URL}]`);
redis.on('connect', function () {
//  client.connect((err) => {
//    if(err) {
//      return console.error('could not connect to postgres', err);
//    } else {
    	rest.post('https://login.salesforce.com/services/oauth2/token', {
    		query: {
    			grant_type:'password',
    			client_id:'3MVG9Rd3qC6oMalUd.EEm8FrmpaPkQs.Jb6CpcCMWu4CKLSmevbJsPy5EALngHRwoS13Zlv37VyvuHMVwScZD',
    			client_secret:'2727761931602693303',
    			username: process.env.SF_USERNAME,
    			password: process.env.SF_PASSWORD
    		}}).on('complete', function(oauthres) {

      		if (oauthres.access_token && oauthres.instance_url) {

            // Import to Redis
            console.log ('Starting import......');
            importAffProfile(oauthres).then (succ => {
              console.log ('importAffProfile done '+ JSON.stringify(succ));
              importItems(oauthres).then (succ => {
                console.log ('importItems done '+ JSON.stringify(succ));
                importAffRules(oauthres).then (succ => {
                  console.log ('importAffRules done '+ JSON.stringify(succ));
                  redis.disconnect();
  //                client.end();
                }, rej => {
                  console.error ('importAffRules rejection : ' + rej);
                  redis.disconnect();
  //                client.end();
                }).catch (err => {
                  console.error ('importAffRules error ' + err);
                  redis.disconnect();
  //                client.end();
                });

              }, rej => {
                console.error ('importItems rejection : ' + rej);
                redis.disconnect();
  //              client.end();
              }).catch (err => {
                console.error ('importItems error ' + err);
                redis.disconnect();
  //              client.end();
              });

            }, rej => {
              console.error ('importAffProfile rejection : ' + rej);
              redis.disconnect();
  //            client.end();
            }).catch (err => {
              console.error ('importAffProfile error ' + err);
              redis.disconnect();
//              client.end();
            });

          } else {
            console.error('no salesforce');
            redis.disconnect();
//            client.end();
          }
        });
//    }
//  });
});

redis.on('error', function (e) {
  console.error ('Redis error',e);
//  client.end();
});
