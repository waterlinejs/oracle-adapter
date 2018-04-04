var adapter = require('../../dist/adapter'),
  should = require('should'),
  support = require('./support/bootstrap'),
  BPromise = require('bluebird');

describe('adapter', function () {

  /**
   * Setup and Teardown
   */

  before(function (done) {
    support.Setup('test_query', done);
  });

  after(function (done) {
    support.Teardown('test_query', done);
  });

  /**
   * QUERY
   *
   * Returns an oracledb recordset form a raw sql query
   */

  describe('.query()', function () {

    describe('should execute raw SQL statement', function () {

      before(function (done) {
        support.Seed('test_query', done);
      });

      describe('select "id" from "test_query"', function () {
        it('should return the oracldb record set', function (done) {
          adapter.query('test', 'test_query', 'select "id" from "test_query"', function (err, results) {
            results.metaData.length.should.eql(1);
            results.rows.length.should.eql(1);
            results.rows[0].id.should.eql(1);
            done()
          });
        });
      });

    });

  });
});
