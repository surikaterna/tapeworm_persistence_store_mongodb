var uuid = require("node-uuid").v4;
var Promise = require("bluebird");
var MongoClient = require('mongodb').MongoClient;
var EventStore = require('tapeworm');
var _ = require('lodash');
var Store = require('..');
var Commit = EventStore.Commit;
var Event = EventStore.Event;
var PersistenceConcurrencyError = EventStore.ConcurrencyError;
var PersistenceDuplicateCommitError = EventStore.DuplicateCommitError;

describe('indexeddb_persistence', () => {
  var _db = null;
  var _client = null;
  function getDb() {
    return _db;
  }
  beforeAll(done => {
    MongoClient.connect("mongodb://localhost:27017", { useUnifiedTopology: true }, function (err, client) {
      _client = client;
      _db = client.db("db_test_suite");
      done(err);
    });
  });
  beforeEach(done => {
    var rdone = _.after(2, done);
    _db.collection('tw_1_commits').drop(function () {
      rdone();
    });
    _db.collection('tw_1_snapshots').drop(function () {
      rdone();
    });
  });

  afterEach(done => {
    /*		_db.dropDatabase(function() {
          done();
        });
    */
    done();
  });

  afterAll(done => {
    _client.close(function () {
      done();
    });
  })

  describe('#commit', () => {

    it('should accept a commit and store it', done => {
      var store = new Store(getDb());
      store.openPartition('1').then(function (partition) {
        var events = [new Event(uuid(), 'type1', { test: 11 })];
        var commit = new Commit(uuid(), 'master', '1', 0, events);
        partition.append(commit).then(function () { return partition.queryAll() }).then(function (x) {
          expect(x.length).toBe(1);
          done();
        }).catch(function (err) {
          done(err);
        });
      });
    });

    it('commit in one stream is not visible in other', done => {
      var store = new Store(getDb());
      store.openPartition('1').then(function (partition) {
        var events = [new Event(uuid(), 'type1', { test: 11 })];
        var commit = new Commit(uuid(), 'master', '1', 0, events);
        partition.append(commit);

        var events = [new Event(uuid(), 'type2', { test: 22 })];
        var commit = new Commit(uuid(), 'master', '2', 0, events);

        partition.append(commit).then(function () {
          Promise.join(partition.queryStream('1'), partition.queryStream('2'), function (r1, r2) {
            expect(r1.length).toBe(1);
            expect(r2.length).toBe(1);
            done();
          }).catch(function (err) {
            done(err);
          });
        });
      });
    });

    it('two commits in one stream are visible', done => {
      var store = new Store(getDb());
      store.openPartition('1').then(function (partition) {
        var events = [new Event(uuid(), 'type1', { test: 11 })];
        var commit = new Commit(uuid(), 'master', '1', 0, events);
        partition.append(commit, function () {
          var events = [new Event(uuid(), 'type2', { test: 22 })];
          var commit = new Commit(uuid(), 'master', '1', 1, events);
          partition.append(commit, function () {
            partition.queryAll().then(function (res) {
              expect(res.length).toBe(2);
              done();
            });
          });
        });
      });
    });
  });

  it('query stream from version without fallback', done => {
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
            expect(res[0].events[0].version).toBe(4);
            expect(res[0].events[1].version).toBe(5);
            expect(res[0].commitSequence).toBe(1)
            done();
          })
        });
      });
    });
  });

  it('query stream from version lead to empty results', done => {
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
            expect(res).toEqual([]);
            done();
          })
        });
      });
    });
  });

  it('query stream from version with fallback', done => {
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
            expect(res.length).toBe(2)
            expect(res[0].events[0].version).toBe(2);
            expect(res[1].events[2].version).toBe(5);
            expect(res[0].commitSequence).toBe(0)
            expect(res[1].commitSequence).toBe(1)
            done();
          })
        });
      });
    });
  });

  it('#getLatestCommit', done => {
    var store = new Store(getDb());
    store.openPartition('1').then(function (partition) {
      var streamId = uuid();
      partition.getLatestCommit(streamId).then((res) => {
        expect(res).toBeUndefined()
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
              expect(res.commitSequence).toBe(1)
              expect(res.events.length).toBe(3);
              expect(res.events[2].data.test).toBe(16);
              done();
            })
          });
        });
      });
    });
  });

  it('#truncateStreamFrom', done => {
    var store = new Store(getDb());
    store.openPartition('1').then(function (partition) {
      var firstStreamId = uuid();
      var secondStreamId = uuid();
      var promises = [];
      promises.push(partition.append(new Commit(uuid(), 'master', firstStreamId, 0, [new Event(uuid(), 'event-1-1', { test: 11, version: 0 })])));
      promises.push(partition.append(new Commit(uuid(), 'master', secondStreamId, 0, [new Event(uuid(), 'event-2-1', { test: 21, version: 0 })])));
      promises.push(partition.append(new Commit(uuid(), 'master', firstStreamId, 1, [new Event(uuid(), 'event-1-2', { test: 12, version: 1 })])));
      promises.push(partition.append(new Commit(uuid(), 'master', secondStreamId, 1, [new Event(uuid(), 'event-2-2', { test: 22, version: 1 })])));

      Promise.all(promises).then(async () => {
        await partition.truncateStreamFrom(firstStreamId, 1); // truncate from sequence 1
        var r1 = await partition.queryStream(firstStreamId);
        var r2 = await partition.queryStream(secondStreamId);
        expect(r1.length).toBe(1);
        expect(r1[0].commitSequence).toBe(0); // seqeunce 0 should exist
        expect(r2.length).toBe(2); // secondStream should not be affected

        await partition.truncateStreamFrom(secondStreamId, -1); // truncate all
        r1 = await partition.queryStream(firstStreamId);
        r2 = await partition.queryStream(secondStreamId);
        expect(r1.length).toBe(1); // firstStream should not be affected
        expect(r2.length).toBe(0); // should be empty

        done();
      });
    });
  });

  describe('#snapshot', () => {
    it('should return previously stored snapshot', done => {
      var store = new Store(getDb());
      store.openPartition('1').then(function (part) {
        part.storeSnapshot('stream1', { iAmSnapshot: true }, 10).then(function (snapshot) {
          part.loadSnapshot('stream1').then(function (newSnapshot) {
            expect(newSnapshot).toEqual(snapshot);
            done();
          });
        });
      });
    });
  });
  describe('#concurrency', () => {
    it('same commit sequence twice should throw', done => {
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
  describe('#duplicateEvents', () => {
    it('same commit twice should throw', done => {
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
  describe('#partition', () => {
    it(
      'getting the same partition twice should return same instance',
      done => {
        var store = new Store(getDb());
        Promise.join(store.openPartition('1'), store.openPartition('1'), function (p1, p2) {
          expect(p1).toBe(p2);
          done();
        });
      }
    );
    it(
      'not indicating partition name should give master partition',
      done => {
        var store = new Store(getDb());
        Promise.join(store.openPartition(), store.openPartition('master'), function (p1, p2) {
          expect(p1).toBe(p2);
          done();
        });
      }
    );
  });
});
