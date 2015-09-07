var adapter = require('../../dist/adapter'),
  should = require('should'),
  support = require('./support/bootstrap');

describe('adapter define', function() {

  /**
   * Setup and Teardown
   */

  before(function(done) {
    support.registerConnection(['test_define', 'user'], function() {
      done();
    });
  });

  after(function(done) {
    support.Teardown('test_define', done);
  });

  // Attributes for the test table
  var definition = {
    id: {
      type: 'number',
      autoIncrement: true
    },
    name: {
      type: 'string',
      notNull: true
    },
    email: 'string',
    title: 'string',
    phone: 'string',
    type: 'string',
    favoriteFruit: {
      defaultsTo: 'blueberry',
      type: 'string'
    },
    age: 'number'
  }

  /**
   * DEFINE
   *
   * Create a new table with a defined set of attributes
   */

  describe('.define()', function() {

    describe('basic usage', function() {

      // Build Table from attributes
      it('should build the table', function(done) {

        adapter.define('test', 'test_define', definition, function(err) {
          if (err) return done(err)
          adapter.describe('test', 'test_define', function(err, result) {
            Object.keys(result).length.should.eql(8);
            done();
          });
        });

      });

      // notNull constraint
      it('should add a notNull constraint', function(done) {
        support.Client(function(err, client) {
          var query = `SELECT column_name
                         FROM USER_TAB_COLUMNS
                         WHERE table_name = 'test_define'
                         and NULLABLE = 'N'`

          client.execute(query, [], function(err, result) {
            result.rows[0].COLUMN_NAME.should.equal('name')

            client.release((err) => {
              if (err) console.log('err?', err)
            })

            done()
          });
        });
      });

    });

    describe('reserved words', function() {

      after(function(done) {
        support.Client(function(err, client, close) {
          var query = 'DROP TABLE "user"';
          client.execute(query, [], function(err) {

            // close client
            client.release((err) => {
              if (err) console.log('err?', err)
            })

            done();
          });
        });
      });

      // Build Table from attributes
      it('should escape reserved words', function(done) {

        adapter.define('test', 'user', definition, function(err) {
          adapter.describe('test', 'user', function(err, result) {
            Object.keys(result).length.should.eql(8);
            done();
          });
        });

      });

    });

  });
});
