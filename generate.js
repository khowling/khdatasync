"use strict"

var benchrest = require('bench-rest'),
		rest = require('restler'),
		sfQueryAll = require('./lib/sfapi.js').sfQueryAll;

// ----------------- Items

var slip= {
	    "CompanyID": "",
	    "StoreID": "0420",
	    "EndDateTime": "2015-11-19T13:23:57",
	    "TotalAmount": 0,
	    "Sale": [],
			"Tender": [{
					"TenderDescription": "Bar",
					"Amount": 0
			}],
			"Customer": {
					"CustomerID": "",
					"AuthorizationMiscSettlementData": ""
			},
			"Discount": [{
					"Amount": 5030.51,
					"DiscountableAmount": 33536.71,
					"DiscountName": "Weißwurstrabatt",
					"PromotionID": "08154711"
			}]
	};
var slipSale = {
			"CodeInput": "1234567890",
			"ItemID": "",
			"Quantity": 0,
			"ExtendedAmount": 0,
			"OriginalAmount": 0,
			"ScanInput": "0003843789012",
			"CurrentUnitPrice": 0,
			"ItemDescription": "",
			"AttributeValue": "EUR",
			"Discount": {
					"Amount": 0,
					"DiscountableAmount": 0,
					"DiscountName": "Weißwurstrabatt",
					"PromotionID": "08154711"
			}
	};

function random (array) {
	return array[randomInt(0, array.length -1)];
}
function randomInt (low, high) {
		let num = Math.floor(Math.random() * (high - low + 1)) + low;
		//console.log ('ranInt : ' + num);
		return num;
}

var oauth, testid, items, profiles, flow = {
	before: [
		{ post: `${process.env.AFFINITY_URL}/authenticate`,
			body: `username=${process.env.SF_USERNAME}&password=${process.env.SF_PASSWORD}`,
			headers: {
				'Content-Type': 'application/x-www-form-urlencoded',
				'Accept': '*/*'
			},
			afterHooks: [(all) => {
				oauth = JSON.parse(all.body);
				return all;
			}]
		}
	],
	main: [
		{ beforeHooks: [(all) => {
				let s ={}, itteration = all.env.index; //(1-100?)
				Object.assign (s, slip);
				s.CompanyID = testid;
				s.Customer.CustomerID = random(profiles).CustomerCardID__c;
				s.Customer.AuthorizationMiscSettlementData = random(['BAR', 'CREDITCARD', 'GIROCARD','Cheque']);
				s.EndDateTime = new Date().toISOString().substr(0,19);
				s.TotalAmount = 0.0;
				s.Sale = []
				let nolines = randomInt(1, 12);

				for (let i = 0; i < nolines; i++) {
					let si = {};
					Object.assign (si, slipSale);
					si.ItemID = random(items).ItemNmb__c;
					si.Quantity = randomInt(1, 5);
					let price = randomInt (90, 999)/100;
					si.ExtendedAmount = price;
					si.OriginalAmount = price;
					si.CurrentUnitPrice = price - (randomInt (9, 80)/100);
					// totals
					s.TotalAmount += (si.Quantity * si.CurrentUnitPrice);
					s.Tender[0].Amount = s.TotalAmount;
					s.Sale.push(si);
				}
				//console.log ('DEBUG : ' + JSON.stringify(s, null, 2));
				all.requestOptions.json =  { 'Transaction': s};
				all.requestOptions.headers = {
					'Content-Type': 'application/json',
					'x-key': oauth.user.signature,
					'x-access-token': oauth.token
				};
				return all;
			}],
			post: `${process.env.AFFINITY_URL}/slip/inject`
		}
	]
};

testid = `Bench${Date.now().toString().substr(6)}`;
console.log ('BenchTest id :  ' + testid);
rest.post('https://login.salesforce.com/services/oauth2/token', {
  query: {
    grant_type:'password',
    client_id:'3MVG9Rd3qC6oMalUd.EEm8FrmpaPkQs.Jb6CpcCMWu4CKLSmevbJsPy5EALngHRwoS13Zlv37VyvuHMVwScZD',
    client_secret:'2727761931602693303',
    username: process.env.SF_USERNAME,
    password: process.env.SF_PASSWORD
  }}).on('complete', function(oauthres) {
  	sfQueryAll('select ItemNmb__c from Item__c', oauthres). then( succ => {
			console.log ('Import Item__c : ' + succ.length);
			items = succ;
			sfQueryAll("select CustomerCardID__c from AffinityProfile__c where CustomerCardID__c NOT IN ('4004233290319','4004233216937','4004233213561','4004233243568','4004233226431','4004233222228','4004233274524','4004233289450')", oauthres). then( succ => {
				profiles = succ;
				console.log ('Import AffinityProfile__c : ' + succ.length);
				let opts = {
 			    limit: 5,     // concurrent connections
 			    iterations: 500  // number of iterations to perform
				};
				console.log ('Starting Bench ' + JSON.stringify(opts));

				benchrest(flow, opts).on('error', function (err, ctxName) {
					console.error('Failed in %s with err: ', ctxName, err);
				}).on('end', function (stats, errorCount) {
			      console.log('error count: ', errorCount);
			      console.log('stats', stats);
			  });
			});
		});
	});
