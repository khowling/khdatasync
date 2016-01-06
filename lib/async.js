"use strict"
module.exports = function(fn, ...args) {

  var generatorfn = fn(...args);

  function handle(result){
    if (result.done) {
      //console.log ('got final yeild val : ' + JSON.stringify(result));
      return Promise.resolve(result.value);
    } else
      return Promise.resolve(result.value).then((res) => {
        //console.log ('got yeild val promise complete : ' + JSON.stringify(res));
        return handle(generatorfn.next(res));
      }, err => {
        console.log (`async got error : ${err}` );
        return Promise.reject(err);
      });
  }
  try {
    return handle(generatorfn.next());
  } catch (ex) {
    console.log ("async catch error " + ex);
    return Promise.reject(ex);
  }
}
