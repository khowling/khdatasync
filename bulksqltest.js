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


// ------------------------------------------- AZURE SQL
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
var SQLTABLES = {
  'slip':
    {schema: 'dbo',table: 'AzureSlip',  head: true,
    fields:    ['companyid', 'customercardid','storeid','enddatetime','totalamount','tsreceived','tsqueueinsert','tsworkerreceived','tsstartcrunsh','tsendcrunsh'],
    sourceMap: ['Transaction.CompanyID','Transaction.Customer.CustomerID','Transaction.StoreID','Transaction.EndDateTime','Transaction.TotalAmount','tsreceived','tsqueueinsert','tsworkerreceived','tsstartcrunsh','tsendcrunsh']
  },
  'slipitem':
    {schema: 'dbo',table: 'AzureSlipItem', head: false, itterator: 'Transaction.Sale',
    fields:    ['codeinput','itemid', 'scaninput','quantity','currentunitprice','extendedamount','originalamount','attributevalue','sfdcitemid','dicountamount','discountableamount','discountname','promotionid'],
    sourceMap: ['CodeInput','ItemID','ScanInput','Quantity','CurrentUnitPrice','ExtendedAmount','OriginalAmount','AttributeValue','Item.sfid','Discount.DiscountAmount','Discount.DiscountableAmount','Discount.DiscountName','Discount.PromotionID']
  }
};
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
            console.log (squery);
            let qrequest = new Request(squery, function(err) {
                  if (err) {
                    console.log('select error : ' + err);
                    reject(err);
                  } else {
                    resolve (slipsidx_recid);
                  }
                });

            qrequest.on('row', function (columns) {
              console.log (`columns: ${JSON.stringify(columns)}`);
              slipsidx_recid.set(uuid_hidx.get(columns[0].value), columns[1].value);
              console.log (`(${columns[0].value}) set ${uuid_hidx.get(columns[0].value)} :  ${columns[1].value}`);
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
          console.log ('adding row : ' + JSON.stringify(posrow));
          bulkLoad.addRow(posrow);
        }
      }
      sql.execBulkLoad(bulkLoad);
    } else
      resolve([]);
  });
}



function exportSlipsMain(connection) {
  return new Promise ((resolve, reject) => {
    let rkey = "slips";
    console.log (`exportSlipsMain : look for ${rkey}`);

    async(exportSlips, rkey).then((redisprofiles) => {
    let popped = redisprofiles.popped, formatted_out =  redisprofiles.formatted_out;
    if (formatted_out.length >0) {
      sqlInsertSlipsBulk (connection, SQLTABLES.slip, formatted_out). then(headmap => {
        console.log (`sqlInsertSlipsBulk inserted headers :  ${headmap.size}`);
        sqlInsertSlipsBulk (connection, SQLTABLES.slipitem, formatted_out, headmap).then(linescnt => {
          console.log (`sqlInsertSlipsBulk inserted lines :  ${linescnt}`);
          redis.del(popped, () => {
                resolve ("Done");
          });
        }, rej => {
          redis.sadd(rkey, popped); // put back popped
          reject ('sqlInsertSlipsItems rejection : ' + rej);
        }).catch (err => {
          redis.sadd(rkey, popped); // put back popped
          reject ('sqlInsertSlipsItems error ' + err);
        });
      }, rej => {
        redis.sadd(rkey, popped); // put back popped
        reject ('sqlInsertSlips rejection : ' + rej);
      }).catch (err => {
        redis.sadd(rkey, popped); // put back popped
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
      redis.disconnect();
    } else {
      exportSlipsMain(connection).then (succ => {
        console.log (`exportSlipsMain success :  ${JSON.stringify(succ)}`);
        redis.disconnect();
        connection.close();
      }, err => {
        console.error (`exportPromotionsMain failed :  ${JSON.stringify(err)}`);
        redis.disconnect();
        connection.close();
      });
    }
  });
});
