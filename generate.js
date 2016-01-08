"use strict"

var benchrest = require('bench-rest'),
		rest = require('restler');

// ----------------- Items
let importItems = function(q, oauth, nextRecordsUrl, retrow) {
	return new Promise(function (resolve, reject)  {
    if (!retrow)
      retrow = [];

		// query for ALL salesforce records
		rest.get (oauth.instance_url + ( nextRecordsUrl || '/services/data/v35.0/query'), {
			headers: {'Authorization': 'Bearer ' +oauth.access_token},
			query: {q: q}
		}).on('complete', (sfrecs) => {
//			console.log ('val : ' + JSON.stringify(val));
			if (sfrecs.records && sfrecs.records.length >0) {
        if (sfrecs.done)
          resolve(retrow.concat(sfrecs.records));
        else
          importItems (q, oauth, sfrecs.nextRecordsUrl, sfrecs.records).then(resolve);
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


var slip= {
    "Transaction": {
        "CompanyID": "Bench-Test",
        "StoreID": "0420",
        "EndDateTime": "2015-11-19T13:23:57",
        "TotalAmount": 28506.20,
        "Sale": [],
				"Tender": [{
						"TenderDescription": "Bar",
						"Amount": 28516.20
				}],
				"Customer": {
						"CustomerID": "4004555552797",
						"AuthorizationMiscSettlementData": "GIROCARD"
				},
				"Discount": [{
						"Amount": 5030.51,
						"DiscountableAmount": 33536.71,
						"DiscountName": "Weißwurstrabatt",
						"PromotionID": "08154711"
				}]
		}
	};
var slipSale = {
			"CodeInput": "0003843789012",
			"ItemID": "0003843",
			"Quantity": 37,
			"ExtendedAmount": 2932.40,
			"OriginalAmount": 3449.88,
			"ScanInput": "0003843789012",
			"CurrentUnitPrice": 93.24,
			"ItemDescription": "COLA CAO 1.2 KGS+10% GRATIS",
			"AttributeValue": "EUR",
			"Discount": {
					"Amount": 517.48,
					"DiscountableAmount": 3449.88,
					"DiscountName": "Weißwurstrabatt",
					"PromotionID": "08154711"
			}
	};

var flow = {
	before: [
		{ post: `${process.env.AFFINITY_URL}/authenticate`,
			body: `username=${process.env.SF_USERNAME}&password=${process.env.SF_PASSWORD}`,
			headers: {'Content-Type': ' application/json'},
			afterHooks: [(all) => {
				console.log ('after hook : ' + JSON.stringify(all));
				all.iterCtx = {auth: JSON.parse(all.body)};
				return all;
			}]
		}
	],
	main: [
		{ post: `${process.env.AFFINITY_URL}/slip/inject`,
			beforeHooks: [(all) => {
				all.requestOptions.json =  slip;
				all.requestOptions.headers = {
					'Content-Type': 'application/json',
					'x-key': all.iterCtx.auth.user.signature,
					'x-access-token': all.iterCtx.auth.token
				}
				return all;
			}]
		}
	]
};


console.log ('Connected Salesforce ......');
rest.post('https://login.salesforce.com/services/oauth2/token', {
  query: {
    grant_type:'password',
    client_id:'3MVG9Rd3qC6oMalUd.EEm8FrmpaPkQs.Jb6CpcCMWu4CKLSmevbJsPy5EALngHRwoS13Zlv37VyvuHMVwScZD',
    client_secret:'2727761931602693303',
    username: process.env.SF_USERNAME,
    password: process.env.SF_PASSWORD
  }}).on('complete', function(oauthres) {
	 console.log ('val : ' + JSON.stringify(oauthres));
//   importItems('select Id, ItemNmb__c, Store__c, WGI__c, ItemFamily__c, SubItemGroup__c from Item__c', oauthres). then( succ => {
//		 console.log ('importItems : ' + succ.length);
//		 importItems('select Id, ItemNmb__c, Store__c, WGI__c, ItemFamily__c, SubItemGroup__c from Item__c', oauthres). then( succ => {
//			 console.log ('importItems : ' + succ.length);
			  benchrest(flow, {
 			    limit: 1,     // concurrent connections
 			    iterations: 1  // number of iterations to perform
 					})
			    .on('error', function (err, ctxName) { console.error('Failed in %s with err: ', ctxName, err); })
			    .on('end', function (stats, errorCount) {
			      console.log('error count: ', errorCount);
			      console.log('stats', stats);
			    });
//		 });
//   });
 });
