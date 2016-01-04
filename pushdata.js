"use strict"

var Redis = require('ioredis'),
    redis = new Redis(process.env.REDIS_URL),
    pg = require('pg'),
    client = new pg.Client(process.env.PG_URL),
    rest = require('restler');

//var primose_redis = function (op, ...opags) {
//  redis[op](opags, ())
//}

function* procRecords(oauth, xref) {
    console.log ('procRecords');
    let custids = yield redis.srandmember("promotions", 10000);
    console.log ('got: ' + custids.length);
    let ret = [];
    for (let p of custids) {
      res = yield redis.hgetall (`profile:${p}:buckets`);
      ret.push (res);
    }
    return ret;
}

var runProcRecords = function(oauth, xref) {
  return new Promise((resolve, reject) => {
    var generator = procRecords(oauth, xref);

    function handle(result){
      console.log ('got result ' + JSON.stringify(result));
      if (result.done)
        return Promise.resolve(result.value);
      else
        return Promise.resolve(result.value).then(res => {
          console.log ('got result ' + JSON.stringify(res));
          return handle(generator.next(res));
        }, err => {
          console.log (`async got error  : ${err}` );
            return handle(generator.throw(err));
            //return handle(generator.next({error: `${err}`}));
        });
    }
    return handle(generator.next());
  });
}


console.log ('Connecting......');
console.log ('connect redis');
redis.on('connect', function () {

  client.connect((err) => {
    if(err) {
      return console.error('could not connect to postgres', err);
    } else {
      console.log ('query pg');
      client.query('SELECT refid, customercardid from affinityprofilemapping', (err, pgRecs) =>{
        if(err) {
          return console.error('could not query postgres', err);
          client.end();
        } else {
          let xref = new Map();
          for (let r of pgRecs.rows) {
            xref.set (r.customercardid, r.refid);
          }

          console.log (`got ${xref.size} profiles,  connect salesforce`);
        	rest.post('https://login.salesforce.com/services/oauth2/token', {
        		query: {
        			grant_type:'password',
        			client_id:'3MVG9Rd3qC6oMalUd.EEm8FrmpaPkQs.Jb6CpcCMWu4CKLSmevbJsPy5EALngHRwoS13Zlv37VyvuHMVwScZD',
        			client_secret:'2727761931602693303',
        			username: process.env.SF_USERNAME,
        			password: process.env.SF_PASSWORD
        		}}).on('complete', function(oauthres) {

          		if (oauthres.access_token && oauthres.instance_url) {
                console.log ('runProcRecords');
                runProcRecords(oauthres, xref).then(() => {
                  redis.disconnect();
                  client.end();
                })
              } else {
                console.error('no salesforce');
                redis.disconnect();
                client.end();
              }
            });
          }
      });
    }
  });
});
redis.on('error', function (e) {
  client.end();
  console.log ('error ' + e);
});
