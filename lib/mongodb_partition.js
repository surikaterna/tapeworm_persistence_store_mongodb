var Promise = require('bluebird');
var ConcurrencyError = require('tapeworm').ConcurrencyError;
var DuplicateCommitError = require('tapeworm').DuplicateCommitError;

var CONCURRENCY_EXCEPTION_CODE = 11000;

var MdbPartition = function (mongodb, partitionId) {
  this._mongodb = mongodb;
  this._partitionId = partitionId;
  this._collection = null;
}

MdbPartition.prototype.open = function () {
  var self = this;
  this._commits = this._mongodb.collection('tw_' + this._partitionId + "_commits");
  this._snapshots = this._mongodb.collection('tw_' + this._partitionId + "_snapshots");
  return new Promise(function (resolve, reject) {
    self._snapshots.ensureIndex({ id: 1 }, { unique: true });
    self._commits.ensureIndex({ id: 1 }, { unique: true }, function (err, indexName) {
      if (err) {
        reject(err);
      } else {
        self._commits.ensureIndex({ streamId: 1, commitSequence: 1 }, { unique: true }, function (err, indexName) {
          if (err) {
            reject(err);
          } else {
            self._commits.ensureIndex({ createDateTime: 1 }, { unique: false }, function (err, indexName) {
              if (err) {
                reject(err);
              } else {
                resolve(self);
              }
            });
          }
        });
      }
    });
  });
};


MdbPartition.prototype.storeSnapshot = function (streamId, snapshot, version, callback) {
  var self = this;
  return new Promise(function (resolve, reject) {
    var toStore = { _id: streamId, id: streamId, version: version, snapshot: snapshot };
    self._snapshots.save(toStore, function (err, res) {
      if (err) {
        reject(err);
      } else {
        resolve(toStore);
      }
    });
  }).nodeify(callback);
};

MdbPartition.prototype.loadSnapshot = function (streamId, callback) {
  var self = this;
  return new Promise(function (resolve, reject) {
    self._snapshots.find({ id: streamId }).toArray(function (err, res) {
      if (err) {
        reject(err);
      } else {
        resolve(res[0]);
      }
    });
  }).nodeify(callback);
}

MdbPartition.prototype.append = function (commit, callback) {
  var self = this;
  commit.isDispatched = false;
  commit.createDateTime = new Date();

  return new Promise(function (resolve, reject) {

    self._commits.insert(commit, function (err, result) {
      if (err) {
        if (err.code === CONCURRENCY_EXCEPTION_CODE) {
          //console.log(JSON.stringify(err));
          if (err.message.indexOf('commitSequence') > 0) {
            reject(new ConcurrencyError('Concurrency error on stream ' + commit.streamId));
          } else {
            reject(new DuplicateCommitError('Concurrency error on stream ' + commit.streamId));
          }
        } else {
          reject(err);
        }
      } else {
        resolve(commit);
      }
    });
  });
}

MdbPartition.prototype.markAsDispatched = function (commit, callback) {
  throw Error('not implemented');
};

MdbPartition.prototype.getUndispatched = function (callback) {
  throw Error('not implemented');
};



MdbPartition.prototype.queryAll = function (callback) {
  //return this._promisify(this._commits.slice(), callback);
  var self = this;
  return new Promise(function (resolve, reject) {
    //TODO: sort in insert order...
    self._commits.find({}).sort({ createDateTime: 1 }).toArray(function (err, commits) {
      if (err) {
        reject(err);
      } else {
        resolve(commits);
      }
    });
  }).nodeify(callback);
};

MdbPartition.prototype.queryStream = function (streamId, fromEventSequence, callback) {
  if (typeof fromEventSequence === 'function') {
    callback = fromEventSequence;
    fromEventSequence = 0;
  }
  var self = this;
  return new Promise(function (resolve, reject) {
    //TODO: sort in insert order...
    self._commits.find({ streamId: streamId }).sort({ commitSequence: 1 }).toArray(function (err, commits) {
      if (err) {
        reject(err);
      } else {
        var result = commits
        if (fromEventSequence > 0) {
          var startCommitId = 0;
          var foundEvents = 0;
          for (var i = 0; i < result.length; i++) {
            foundEvents += result[0].events.length;
            startCommitId++;
            if (foundEvents >= fromEventSequence) {
              break;
            }
          }
          var tooMany = foundEvents - fromEventSequence;

          result = result.slice(startCommitId - (tooMany > 0 ? 1 : 0));
          if (tooMany > 0) {
            result[0].events = result[0].events.slice(result[0].events.length - tooMany);
          }
        }
        resolve(result);
      }
    });
  }).nodeify(callback);
};


module.exports = MdbPartition;