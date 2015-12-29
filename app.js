
"use strict"

var rest = require('restler');
var pg = require('pg'),
	  client = new pg.Client(process.env.PG_URL);

let importData = function(pg, oauth, syncdef, nextRecordsUrl) {
	return new Promise(function (resolve, reject)  {
		if (!nextRecordsUrl)
			process.stdout.write(`\n[${syncdef.table}] processing .`);
		else
			process.stdout.write('.');
		rest.get (oauth.instance_url + ( nextRecordsUrl || '/services/data/v35.0/query'), {
			headers: {
				'Authorization': 'Bearer ' +oauth.access_token,
				'Sforce-Query-Options': 'batchSize=500'
			},
			query: {
				q:'select Id,'+ syncdef.fields.join(',') +'  from '+syncdef.table+' '
			}
		}).on('complete', (val) => {
//			console.log ('val : ' + JSON.stringify(val));
			if (val.records && val.records.length >0) {
				let insstr = `INSERT INTO ${syncdef.schema}.${syncdef.table} (id,sfid,${syncdef.fields.join(',').toLowerCase()}) VALUES `,
						valpos = [], valarray = [];
				for (let r of val.records) {
					let posrow = [];
					// id from sequence
					posrow.push (`nextval('${syncdef.schema}.${syncdef.idsequence}')`);
					// sfid from salesforce
					valarray.push (r['Id']);
					posrow.push (`$${valarray.length}`);
					for (let rc of syncdef.fields) {
						if (rc === "CreatedDate")
							valarray.push (new Date(r[rc]));
						else
							valarray.push (r[rc]);
						posrow.push (`$${valarray.length}`);
					}
					valpos.push (`(${posrow.join(', ')})`);
				}
				insstr+= valpos.join(',');
				// insert into pg
				pg.query({text: insstr, values: valarray}, function(err, result) {
					if(err) {
						console.error('error running query', err);
						reject (err);
					} else {
//						console.log ('ok : ' + JSON.stringify(result));
						if (val.done)
							resolve(result);
						else
							importData (pg, oauth, syncdef, val.nextRecordsUrl).then((res) => resolve(res));
					}
				});
			} else {
				console.error('val', err);
				reject ('val ' + val);
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

let SYNC_DEFS = [
	{schema: 'salesforce', idsequence: 'item__c_id_seq',            table: 'item__c',            fields: [ 'IsDeleted', 'CreatedDate', 'Name', 'ItemNmb__c', 'Store__c', 'WGI__c', 'ItemFamily__c', 'SubItemGroup__c', 'Category__c']},
	{schema: 'salesforce', idsequence: 'affinityprofile__c_id_seq', table: 'affinityprofile__c', fields: [ 'IsDeleted', 'CreatedDate', 'Name', 'Customer__c', 'CustomerCardID__c', 'Transfer__c']},
	{schema: 'salesforce', idsequence: 'affinityrule__c_id_seq',    table: 'affinityrule__c',    fields: [ 'IsDeleted', 'CreatedDate', 'Name', 'Active__c', 'GeneratedCode__c']}
]

let importAppData = function(client, oauthres, syncDefs) {
	let p = null;
	for (let sdefs of syncDefs) {
		if (!p)
			p = importData(client, oauthres, sdefs);
		else {
			p = p.then(() => importData(client, oauthres, sdefs));
		}
	}
	return p;
}

let refreshAffProfileMap = function(pg) {
	return new Promise(function (resolve, reject)  {
		pg.query('DELETE FROM affinityprofilemapping', function(err, result) {
			if(err) {
				console.error('error running query', err);
				reject (err);
			} else {
				pg.query('INSERT INTO affinityprofilemapping SELECT customercardid__c customercardid, id refid FROM salesforce.affinityprofile__c', function(err, result) {
					if(err) {
						console.error('error running query', err);
						reject (err);
					} else {
						resolve(result);
					}
				});
			}
		});
	});
}


console.log ('Connecting......');
client.connect(function(err) {
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

//			console.log ("result : " + JSON.stringify(result));
			if (oauthres.access_token && oauthres.instance_url) {
/*
				importAppData (client, oauthres, SYNC_DEFS).then (
					(res) => {
						console.log ('\nDone : ' + + JSON.stringify(res));
						client.end();
					}, (err) => {
						console.log ('\nErr : ' + JSON.stringify(err));
						client.end();
					}
				);
*/
				refreshAffProfileMap (client).then (
					(res) => {
						console.log ('\nDone : ' + + JSON.stringify(res));
						client.end();
					}, (err) => {
						console.log ('\nErr : ' + JSON.stringify(err));
						client.end();
					}
				);

			}
		});
	}
});
