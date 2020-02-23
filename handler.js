'use strict';
const db = require("./db_connect");

module.exports.hello = function (event, context) {
  //console.log(JSON.stringify(event, null, 2));
  event.Records.forEach(function (record) {
    // Kinesis data is base64 encoded so decode here
    var payload = Buffer.from(record.kinesis.data, 'base64').toString('ascii');

    console.log('Decoded payload:', payload);
    db.insert('myTable', payload)
    .then(res => {
      callback(null, {
        statusCode: 200,
        body: "myTable Created!" + res
      })
    })
    .catch(e => {
      callback(null, {
        statusCode: e.statusCode || 500,
        body: "Could not create myTable " + e
      })
    })
  });
};

  // Use this code if you don't use the http event with the LAMBDA-PROXY integration
  // return { message: 'Go Serverless v1.0! Your function executed successfully!', event };


module.exports.getAllMessages = (event, context, callback) => {
  context.callbackWaitsForEmptyEventLoop = false;
  db.getAll('myTable')
    .then(res => {
      callback(null, {
        statusCode: 200,
        body: JSON.stringify(res)
      })
    })
    .catch(e => {
      console.log(e);
      callback(null, {
        statusCode: e.statusCode || 500,
        body: 'Error: Could not find myTable: ' + e
      })
    })
};

module.exports.getMessage = (event, context, callback) => {
  context.callbackWaitsForEmptyEventLoop = false;
  db.getById('myTable', event.pathParameters.id)
    .then(res => {
      callback(null, {
        statusCode: 200,
        body: JSON.stringify(res)
      })
    })
    .catch(e => {
      callback(null, {
        statusCode: e.statusCode || 500,
        body: "Could not find myTable: " + e
      })
    })
};


module.exports.storeMessage = (event, context, callback) => {
  context.callbackWaitsForEmptyEventLoop = false;
  console.log(db)
  const data = JSON.parse(event.body);
  db.insert('myTable', data)
    .then(res => {
      callback(null, {
        statusCode: 200,
        body: "myTable Created!" + res
      })
    })
    .catch(e => {
      callback(null, {
        statusCode: e.statusCode || 500,
        body: "Could not create myTable " + e
      })
    })
};

module.exports.updateMessage = (event, context, callback) => {
  context.callbackWaitsForEmptyEventLoop = false;
  const data = JSON.parse(event.body);
  db.updateById('myTable', event.pathParameters.id, data)
    .then(res => {
      callback(null, {
        statusCode: 200,
        body: "myTable Updated!" + res
      })
    })
    .catch(e => {
      callback(null, {
        statusCode: e.statusCode || 500,
        body: "Could not update myTable" + e
      })
    })
};

module.exports.deleteMessage = (event, context, callback) => {
  context.callbackWaitsForEmptyEventLoop = false;
  db.deleteById('myTable', event.pathParameters.id)
    .then(res => {
      callback(null, {
        statusCode: 200,
        body: "myTable Deleted!"
      })
    })
    .catch(e => {
      callback(null, {
        statusCode: e.statusCode || 500,
        body: "Could not delete myTable" + e
      })
    })
};