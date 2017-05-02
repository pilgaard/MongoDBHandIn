var Promise = require("bluebird");
var program = require('commander');
var MongoClient = require('mongodb').MongoClient, assert = require('assert');

// Connection URL 
var url = 'mongodb://localhost:27017/twitter';

program
  .command('NumUsers')
  .description('Get how many Twitter users there is in our database.')
  .action(function () {
    NumUsers().then((result) => {
      console.log(result);
    });
  });

program
  .command('TopLinkers')
  .description('Get which Twitter users link the most to other Twitter users.')
  .action(function () {
    TopLinkers().then((result) => {
      console.log(result);
    });
  });

program
  .command('TopMentioned')
  .description('Get who is the most mentioned Twitter users.')
  .action(function () {
    TopMentioned().then((result) => {
      console.log(result);
    });
  });

program
  .command('TopActivity')
  .description('Get who are the most active Twitter users.')
  .action(function () {
    TopActivity().then((result) => {
      console.log(result);
    });
  });

program
  .command('TopGrumpyAndPositive')
  .description('Get the five most grumpy and positive Twitter users.')
  .action(function () {
    TopGrumpyAndPositive().then((result) => {
      console.log(result);
    });
  });        

program.parse(process.argv);

function NumUsers() {
  return new Promise((resolve, reject) => {
    MongoClient
      .connect(url)
      .then((db) => {
        db.collection('tweets').aggregate(
          [
            { $group: { _id: "$user" } },
            { $group: { _id: 1, count: { $sum: 1 } } }
          ])
          .toArray()
          .then((result) => {
            db.close();
            resolve(result[0].count);
          })
          .catch((err) => {
            db.close();
            reject(err);
          });
      })
      .catch(function (err) {
        reject(err);
      });
  });
}

function TopLinkers() {
  return new Promise((resolve, reject) => {
    MongoClient
      .connect(url)
      .then((db) => {
        db.collection('tweets').aggregate(
          [
            { $match: { text: { $regex: /@\w+/ } } },
            { $group: { _id: "$user", numLinks: { $sum: 1 } } },
            { $sort: { numLinks: -1 } },
            { $limit: 10 }
          ]).toArray()
          .then((result) => {
            db.close();
            resolve(result);
          })
          .catch((err) => {
            db.close();
            reject(err);
          });
      })
      .catch(function (err) {
        reject(err);
      });
  });
}

function Top5MostGrumpy() {
  return new Promise((resolve, reject) => {
    MongoClient
      .connect(url)
      .then((db) => {
        db.collection('tweets').aggregate(
          [
            { $match: { polarity: { $eq: 0 } } },
            { $group: { _id: "$user", numGrumpyPost: { $sum: 1 } } },
            { $sort: { numGrumpyPost: -1 } },
            { $limit: 5 }
          ])
          .toArray()
          .then((result) => {
            db.close();
            resolve(result);
          })
          .catch((err) => {
            db.close();
            reject(err);
          });
      })
      .catch(function (err) {
        reject(err);
      });
  });
}
function Top5MostPositive() {
  return new Promise((resolve, reject) => {
    MongoClient
      .connect(url)
      .then((db) => {
        db.collection('tweets').aggregate(
          [
            { $match: { polarity: { $eq: 4 } } },
            { $group: { _id: "$user", numHappyPost: { $sum: 1 } } },
            { $sort: { numHappyPost: -1 } },
            { $limit: 5 }
          ])
          .toArray()
          .then((result) => {
            db.close();
            resolve(result);
          })
          .catch((err) => {
            db.close();
            reject(err);
          });
      })
      .catch(function (err) {
        reject(err);
      });
  });
}
function TopGrumpyAndPositive() {
  return Promise.all([Top5MostGrumpy(), Top5MostPositive()]).spread(function (resultGrumpy, resultPositive) {
    let tempResult = {
      "positive": resultPositive,
      "grumpy": resultGrumpy
    };
    return tempResult;
  });
}

function TopMentioned() {
  return new Promise((resolve, reject) => {
    MongoClient
      .connect(url)
      .then((db) => {
        db.collection('tweets').aggregate(
          [
            {
              $project: {
                country: 1,
                words: {
                  $split: ["$text", " "]
                }
              }
            },
            { $unwind: '$words' },
            { $match: { words: { $regex: /@\w+/ } } },
            { $group: { _id: "$words", numMentioned: { $sum: 1 } } },
            { $sort: { numMentioned: -1 } },
            { $limit: 5 }
          ])
          .toArray()
          .then((result) => {
            db.close();
            resolve(result);
          })
          .catch((err) => {
            db.close();
            reject(err);
          });
      })
      .catch(function (err) {
        reject(err);
      });
  });
}

function TopActivity() {
  return new Promise((resolve, reject) => {
    MongoClient
      .connect(url)
      .then((db) => {
        db.collection('tweets').aggregate(
          [
            { $group: { _id: "$user", activity: { $sum: 1 } } },
            { $sort: { activity: -1 } },
            { $limit: 10 }
          ],
          { allowDiskUse: true })
          .toArray()
          .then((result) => {
            db.close();
            resolve(result);
          })
          .catch((err) => {
            db.close();
            reject(err);
          });
      })
      .catch(function (err) {
        reject(err);
      });
  });
}
