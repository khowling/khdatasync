
"use strict"

var rest = require('restler');
var pg = require('pg'),
	  client = new pg.Client(process.env.PG_URL),
		IMPORT_DEFS = [
	//		{schema: 'salesforce', idsequence: 'item__c_id_seq',            table: 'item__c',            fields: [ 'IsDeleted', 'SystemModstamp', 'CreatedDate', 'Name', 'ItemNmb__c', 'Store__c', 'WGI__c', 'ItemFamily__c', 'SubItemGroup__c', 'Category__c'], checkModify: true},
	//		{schema: 'salesforce', idsequence: 'affinityprofile__c_id_seq', table: 'affinityprofile__c', fields: [ 'IsDeleted', 'SystemModstamp', 'CreatedDate', 'Name', 'Customer__c', 'CustomerCardID__c', 'Transfer__c']},
	//		{schema: 'salesforce', idsequence: 'affinityrule__c_id_seq',    table: 'affinityrule__c',    fields: [ 'IsDeleted', 'SystemModstamp', 'CreatedDate', 'Name', 'Active__c', 'GeneratedCode__c'], checkModify: true},
			{schema: 'salesforce', idsequence: 'coupon_zuweisung__c_id_seq',table: 'coupon_zuweisung__c',fields: [ 'IsDeleted', 'SystemModstamp', 'CreatedDate', 'Name', 'Store__c', 'Coupon__c', 'CustomerCardId__c', 'PromotionID__c', 'EIngeloest__c', 'Eingeloest_Am__c', 'ValidFrom__c', 'ValidTo__c' ], checkModify: true}
		];

// ------------------------------------------------------------- Salesforce -> Postgres
let importData = function(pg, oauth, syncdef, nextRecordsUrl) {
	return new Promise(function (resolve, reject)  {
		// query for ALL salesforce records
		rest.get (oauth.instance_url + ( nextRecordsUrl || '/services/data/v35.0/query'), {
			headers: {
				'Authorization': 'Bearer ' +oauth.access_token,
				'Sforce-Query-Options': 'batchSize=500'
			},
			query: {
				q:'select Id,'+ syncdef.fields.join(',') +'  from '+syncdef.table+' '
			}
		}).on('complete', (sfrecs) => {
//			console.log ('val : ' + JSON.stringify(val));
			if (sfrecs.records && sfrecs.records.length >0) {

				// query for existing postgres records (determine if changed or new)
				let posarray = [], valarray = [];
				for (let r of sfrecs.records) {
					valarray.push (r['Id']);
					posarray.push (`$${valarray.length}`);
				}
				pg.query({
					text: `SELECT sfid, ${syncdef.fields.join(',').toLowerCase()} from ${syncdef.schema}.${syncdef.table}  where sfid IN (${posarray.join(', ')})`,
					values: valarray}, function(err, pgRecs) {

					let changeRecs = [], newRecs = [];
					if(err)
						return reject (err);
					else if (pgRecs.rows && pgRecs.rows.length >0) {
						let pgRecsMap = new Map();
						for (let r of pgRecs.rows) {
							pgRecsMap.set(r['sfid'], r);
						}

						for (let sfrec of sfrecs.records) {
							if (pgRecsMap.has(sfrec['Id'])) {
								let pgrec = pgRecsMap.get(sfrec['Id']);
								if (syncdef.checkModify) for (let f of syncdef.fields) {
									if (!(f === "CreatedDate" || f === "SystemModstamp" )) {
										let sfval = sfrec[f], pgval = pgrec[f.toLowerCase()];
										if (f === "Eingeloest_Am__c" || f === "ValidFrom__c" || f === "ValidTo__c")
											sfval = sfval ? new Date(sfval).getTime() : 0, pgval = pgval ? new Date(pgval).getTime() : 0;

										if (sfval !== pgval) {
											console.log (`diff ${sfrec['Id']} ${f}: ${sfval} :: ${pgval}`);
											changeRecs.push (sfrec); break;
										}
									}
								}
							} else {
								newRecs.push (sfrec);
							}
						}
					} else {
						console.log (`all new`);
						newRecs = sfrecs.records;
					}
					console.log (`[${syncdef.table}] sfdc: ${sfrecs.records.length} (insert: ${newRecs.length}, update: ${changeRecs.length})`);
					if (newRecs.length >0) {
						let insstr = `INSERT INTO ${syncdef.schema}.${syncdef.table} (id,sfid,${syncdef.fields.join(',').toLowerCase()}) VALUES `,
								valpos = [], valarray = [];
						for (let r of newRecs) {
							let posrow = [];
							// id from sequence
							posrow.push (`nextval('${syncdef.schema}.${syncdef.idsequence}')`);
							// sfid from salesforce
							valarray.push (r['Id']);
							posrow.push (`$${valarray.length}`);
							for (let rc of syncdef.fields) {
								if (rc === "CreatedDate" || rc === "SystemModstamp" || rc === "Eingeloest_Am__c" || rc === "ValidFrom__c" || rc === "ValidTo__c")
									if (r[rc])
										valarray.push (new Date(r[rc]));
									else
										valarray.push (null);
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
								if (sfrecs.done)
									resolve(result);
								else
									importData (pg, oauth, syncdef, sfrecs.nextRecordsUrl).then((res) => resolve(res));
							}
						});
					} else {
						if (sfrecs.done)
							resolve({});
						else
							importData (pg, oauth, syncdef, sfrecs.nextRecordsUrl).then(resolve);
					}
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


let importAppData = function(client, oauth, syncDefs) {
	let p = null;
	for (let sdefs of syncDefs) {
		if (!p)
			p = importData(client, oauth, sdefs);
		else {
			p = p.then(() => importData(client, oauth, sdefs));
		}
	}
	return p;
}

// ------------------------------------------------------------- Postgres -> Postgres (affinityprofile__c -> affinityprofilemapping)
let refreshAffProfileMap = function(pg) {
	return new Promise(function (resolve, reject)  {
		pg.query('DELETE FROM affinityprofilemapping', function(err, result) {
			if(err)
				reject (err);
			else
				pg.query('INSERT INTO affinityprofilemapping SELECT customercardid__c customercardid, id refid FROM salesforce.affinityprofile__c', function(err, result) {
					if(err)
						reject (err);
					else
						resolve(result);
				});
		});
	}).catch ((e) => console.error ('catch ', e));
}

// ------------------------------------------------------------- Postgres -> Salesforce
let sfdcAffinityProfiles = function(oauth, nextRecordsUrl, savedRecs) {
	return new Promise(function (resolve, reject)  {
		// query for ALL salesforce records
		process.stdout.write('.');
		rest.get (oauth.instance_url + ( nextRecordsUrl || '/services/data/v35.0/query'), {
			headers: {
				'Authorization': 'Bearer ' +oauth.access_token,
				'Sforce-Query-Options': 'batchSize=2000'
			},
			query: {
				q:'select Id, Transfer__c from AffinityProfile__c'
			}
		}).on('complete', (sfrecs) => {
			let recsMap = savedRecs || new Map();
			for (let r of sfrecs.records) {
				recsMap.set(r.Id, r.Transfer__c);
			}
			if (sfrecs.done)
				resolve(recsMap);
			else
				sfdcAffinityProfiles (oauth, sfrecs.nextRecordsUrl, recsMap).then(resolve);
		});
	}).catch ((e) => console.error ('catch ', e));
}


let exportAffinityProfile = function(pg, oauth) {
	return new Promise(function (resolve, reject)  {
		process.stdout.write ('reading sfdcAffinityProfiles ');
		sfdcAffinityProfiles(oauth).then(
			(existingMap) => {
				console.log (existingMap.size);
				pg.query("SELECT sfid, transfer__c from salesforce.affinityprofile__c", function(err, result) {
					if(err)
						reject (err);
					else if (result.rows && result.rows.length >0) {
						console.log ('pg affinityprofile__c ....' + result.rows.length);
						let changes = [];
						for (let r of result.rows) {
							if (existingMap.has(r.sfid)) {
								//console.log (`diff ${existingMap.get(r.sfid)}    ${r.transfer__c}`);
								if (existingMap.get(r.sfid) !== r.transfer__c) {
									changes.push (`${r.sfid},"${r.transfer__c.replace(/"/g, '\""')}"`);
								}
							}
						}
						if (changes.length > 0) {
							console.log ('profiles need updating  : ' + changes.length);
							resolve();

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

								//console.log ('job  ' + JSON.stringify(jobresponse));
								let jobid = jobresponse.jobInfo.id[0];

								let payload = "Id,Transfer__c\n" + changes.join('\n');

								//console.log ('jobid ' + jobid +  ', payload ' + payload);
								rest.post (oauth.instance_url + '/services/async/35.0/job/'+jobid+'/batch', {
									headers: {
										'X-SFDC-Session': oauth.access_token,
										'Content-Type': 'text/csv'
									},
									data : payload
								}).on('complete', (val) => {
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

						} else {
							resolve ("upto date");
						}
					} else
						resolve ("no pg profiles");
				});
			}, (err) => {
				console.log ('sfdcAffinityProfiles nErr : ' + JSON.stringify(err));
				reject (err);
			}
		)
	}).catch ((e) => console.error ('catch ', e));
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


			if (oauthres.access_token && oauthres.instance_url) {
					console.log ("importAppData");
					importAppData (client, oauthres, IMPORT_DEFS).then (
						(res) => {
							console.log ('importAppData Done : ' + JSON.stringify(res));
							console.log ("refreshAffProfileMap");
							refreshAffProfileMap (client).then (
								(res) => {
									console.log ('refreshAffProfileMap Done : ' + JSON.stringify(res));
									console.log ("exportAffinityProfile");
									exportAffinityProfile (client, oauthres).then (
										(res) => {
											console.log ('exportAffinityProfile Done : ' + JSON.stringify(res));
											client.end();
										}, (err) => {
											console.log ('exportAffinityProfile nErr : ' + JSON.stringify(err));
											client.end();
										}
									);
								}, (err) => {
									console.log ('refreshAffProfileMap Err : ' + JSON.stringify(err));
									client.end();
								}
							);
						}, (err) => {
							console.log ('refreshAffProfileMap Err : ' + JSON.stringify(err));
							client.end();
						}
					);
			}
		});
	}
});
