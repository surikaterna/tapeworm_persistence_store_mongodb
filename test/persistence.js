var should = require('should');
var uuid = require("node-uuid").v4;
var Promise = require("bluebird");
var MongoClient = require('mongodb').MongoClient;
var Server = require('mongodb').Server;
var EventStore = require('tapeworm');
var _ = require('lodash');
var Store = require('..');
var Commit = EventStore.Commit;
var Event = EventStore.Event;
var PersistenceConcurrencyError = EventStore.ConcurrencyError;
var PersistenceDuplicateCommitError = EventStore.DuplicateCommitError;

describe('indexeddb_persistence', function () {
  var _db = null;
  function getDb() {
    return _db;
  }
  before(function (done) {
    MongoClient.connect("mongodb://localhost:27017/db_test_suite", function (err, db) {
      _db = db;
      done(err);
    });
  });
  beforeEach(function (done) {
    var rdone = _.after(2, done);
    _db.collection('tw_1_commits').drop(function () {
      rdone();
    });
    _db.collection('tw_1_snapshots').drop(function () {
      rdone();
    });
  });

  afterEach(function (done) {
    /*		_db.dropDatabase(function() {
          done();
        });
    */
    done();
  });

  after(function (done) {
    _db.close(function () {
      done();
    });
  })

  describe('#commit', function () {

    it('should accept a commit and store it', function (done) {
      var store = new Store(getDb());
      store.openPartition('1').then(function (partition) {
        var events = [new Event(uuid(), 'type1', { test: 11 })];
        var commit = new Commit(uuid(), 'master', '1', 0, events);
        partition.append(commit).then(function () { return partition.queryAll() }).then(function (x) {
          x.length.should.equal(1);
          done();
        }).catch(function (err) {
          done(err);
        });
      });
    });

    it('commit in one stream is not visible in other', function (done) {
      var store = new Store(getDb());
      store.openPartition('1').then(function (partition) {
        var events = [new Event(uuid(), 'type1', { test: 11 })];
        var commit = new Commit(uuid(), 'master', '1', 0, events);
        partition.append(commit);

        var events = [new Event(uuid(), 'type2', { test: 22 })];
        var commit = new Commit(uuid(), 'master', '2', 0, events);

        partition.append(commit).then(function () {
          Promise.join(partition.queryStream('1'), partition.queryStream('2'), function (r1, r2) {
            r1.length.should.equal(1);
            r2.length.should.equal(1);
            done();
          }).catch(function (err) {
            done(err);
          });
        });
      });
    });

    it('two commits in one stream are visible', function (done) {
      var store = new Store(getDb());
      store.openPartition('1').then(function (partition) {
        var events = [new Event(uuid(), 'type1', { test: 11 })];
        var commit = new Commit(uuid(), 'master', '1', 0, events);
        partition.append(commit, function () {
          var events = [new Event(uuid(), 'type2', { test: 22 })];
          var commit = new Commit(uuid(), 'master', '1', 1, events);
          partition.append(commit, function () {
            partition.queryAll().then(function (res) {
              res.length.should.equal(2);
              done();
            });
          });
        });
      });
    });
  });

  it('query stream from version without fallback', function (done) {
    var store = new Store(getDb());
    store.openPartition('1').then(function (partition) {
      var streamId = uuid();
      var events = [
        new Event(uuid(), 'event-1', { test: 11, version: 0 }),
        new Event(uuid(), 'event-2', { test: 12, version: 1 }),
        new Event(uuid(), 'event-3', { test: 13, version: 2 })
      ];
      events.forEach(function (e) { e.version = e.data.version }); // fake version...
      var commit = new Commit(uuid(), 'master', streamId, 0, events);
      partition.append(commit).then(function () {
        var events = [
          new Event(uuid(), 'event-4', { test: 14, version: 3 }),
          new Event(uuid(), 'event-5', { test: 15, version: 4 }),
          new Event(uuid(), 'event-6', { test: 16, version: 5 })
        ];
        events.forEach(function (e) { e.version = e.data.version }); // fake version...
        var commit = new Commit(uuid(), 'master', streamId, 1, events);
        partition.append(commit).then(function () {
          partition.queryStream(streamId, 4).then((res) => {
            res[0].events[0].version.should.equal(4);
            res[0].events[1].version.should.equal(5);
            res[0].commitSequence.should.equal(1)
            done();
          })
        });
      });
    });
  });

  it('query stream from version lead to empty results', function (done) {
    var store = new Store(getDb());
    store.openPartition('1').then(function (partition) {
      var streamId = uuid();
      var events = [
        new Event(uuid(), 'event-1', { test: 11, version: 0 }),
        new Event(uuid(), 'event-2', { test: 12, version: 1 }),
        new Event(uuid(), 'event-3', { test: 13, version: 2 })
      ];
      events.forEach(function (e) { e.version = e.data.version }); // fake version...
      var commit = new Commit(uuid(), 'master', streamId, 0, events);
      partition.append(commit).then(function () {
        var events = [
          new Event(uuid(), 'event-4', { test: 14, version: 3 }),
          new Event(uuid(), 'event-5', { test: 15, version: 4 }),
          new Event(uuid(), 'event-6', { test: 16, version: 5 })
        ];
        events.forEach(function (e) { e.version = e.data.version }); // fake version...
        var commit = new Commit(uuid(), 'master', streamId, 1, events);
        partition.append(commit).then(function () {
          partition.queryStream(streamId, 6).then((res) => {
            res.should.eql([]);
            done();
          })
        });
      });
    });
  });

  it('query stream from version with fallback', function (done) {
    var store = new Store(getDb());
    store.openPartition('1').then(function (partition) {
      var streamId = uuid();
      var events = [
        new Event(uuid(), 'event-1', { test: 11, version: 0 }),
        new Event(uuid(), 'event-2', { test: 12, version: 1 }),
        new Event(uuid(), 'event-3', { test: 13, version: 2 })
      ];
      events.forEach(function (e) { e.version = e.data.version }); // fake version...
      var commit = new Commit(uuid(), 'master', streamId, 0, events);
      partition.append(commit).then(function () {
        var events = [
          new Event(uuid(), 'event-4', { test: 14, version: 3 }),
          new Event(uuid(), 'event-5', { test: 15, version: 4 }),
          new Event(uuid(), 'event-6', { test: 16, version: 5 })
        ];
        events.forEach(function (e) { e.version = e.data.version }); // fake version...
        var commit = new Commit(uuid(), 'master', streamId, 1, events);
        partition.append(commit).then(function () {
          partition.queryStream(streamId, 2).then((res) => {
            res.length.should.equal(2)
            res[0].events[0].version.should.equal(2);
            res[1].events[2].version.should.equal(5);
            res[0].commitSequence.should.equal(0)
            res[1].commitSequence.should.equal(1)
            done();
          })
        });
      });
    });
  });

  it('#getLatestCommit', function (done) {
    var store = new Store(getDb());
    store.openPartition('1').then(function (partition) {
      var streamId = uuid();
      partition.getLatestCommit(streamId).then((res) => {
        should.equal(res, undefined)
        var events = [
          new Event(uuid(), 'event-1', { test: 11, version: 0 }),
          new Event(uuid(), 'event-2', { test: 12, version: 1 }),
        ];
        events.forEach(function (e) { e.version = e.data.version }); // fake version...
        var commit = new Commit(uuid(), 'master', streamId, 0, events);
        partition.append(commit).then(function () {
          var events = [
            new Event(uuid(), 'event-4', { test: 14, version: 3 }),
            new Event(uuid(), 'event-5', { test: 15, version: 4 }),
            new Event(uuid(), 'event-6', { test: 16, version: 5 })
          ];
          events.forEach(function (e) { e.version = e.data.version }); // fake version...
          var commit = new Commit(uuid(), 'master', streamId, 1, events);
          partition.append(commit).then(function () {
            partition.getLatestCommit(streamId).then((res) => {
              res.commitSequence.should.equal(1)
              res.events.length.should.equal(3);
              res.events[2].data.test.should.equal(16);
              done();
            })
          });
        });
      });
    });
  });

  describe('#snapshot', function () {
    it('should return previously stored snapshot', function (done) {
      var store = new Store(getDb());
      store.openPartition('1').then(function (part) {
        part.storeSnapshot('stream1', { iAmSnapshot: true }, 10).then(function (snapshot) {
          part.loadSnapshot('stream1').then(function (newSnapshot) {
            JSON.stringify(newSnapshot).should.equal(JSON.stringify(snapshot));
            done();
          });
        });
      });
    });
  });
  describe('#concurrency', function () {
    it('same commit sequence twice should throw', function (done) {
      var store = new Store(getDb());
      store.openPartition('1').then(function (partition) {
        var events = [new Event(uuid(), 'type1', { test: 11 })];
        var commit = new Commit(uuid(), 'master', '1', 0, events);
        var commit2 = new Commit(uuid(), 'master', '1', 0, events);
        return Promise.join(partition.append(commit), partition.append(commit2), function () {
          done(new Error("Should have thrown concurrency error"));
        });
      }).catch(PersistenceConcurrencyError, function (err) {
        done();
      }).catch(function (err) {
        console.log('err' + err);
        done(err);
      });
    });
  });
  describe('#duplicateEvents', function () {
    it('same commit twice should throw', function (done) {
      var store = new Store(getDb());
      store.openPartition('1').then(function (partition) {
        var events = [new Event(uuid(), 'type1', { test: 11 })];
        var commit = new Commit(uuid(), 'master', '1', 0, events);
        partition.append(commit).then(function () {
          return partition.append(commit);
        })
          .then(function () {
            done(new Error("Should have DuplicateCommitError"));
          }).catch(PersistenceDuplicateCommitError, function (err) {
          done();
        }).catch(function (err) {
          done(err);
        });
      });
    });
  });
  describe('#partition', function () {
    it('getting the same partition twice should return same instance', function (done) {
      var store = new Store(getDb());
      Promise.join(store.openPartition('1'), store.openPartition('1'), function (p1, p2) {
        p1.should.equal(p2);
        done();
      });
    });
    it('not indicating partition name should give master partition', function (done) {
      var store = new Store(getDb());
      Promise.join(store.openPartition(), store.openPartition('master'), function (p1, p2) {
        p1.should.equal(p2);
        done();
      });
    });
  });
});
