"use strict"

var rest = require('restler');

function sfQueryAll (q, oauth, nextRecordsUrl, retrow) {
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
				let conrows = retrow.concat(sfrecs.records);
        if (sfrecs.done)
          resolve(conrows);
        else
          sfQueryAll (q, oauth, sfrecs.nextRecordsUrl, conrows).then(resolve);
      } else {
        console.error('sfrec', JSON.stringify(sfrecs));
        resolve (retrow);
      }
    }).on('error', (err) => {
      console.error('error', err);
      reject('err : ' + err);
    }).on ('fail', (err) => {
      console.error('fail', err);
      reject('fail : ' + err);
    });
  }).catch ((e) => console.error ('catch ', e));
};

module.exports =  { sfQueryAll };
