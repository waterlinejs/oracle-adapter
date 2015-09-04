import should from 'should'
import utils from '../../dist/utils'
const results = {
  rows: [
    ['id', 'NUMBER', 'Y'],
    ['name', 'VARCHAR2', 'N'],
    ['email', 'VARCHAR2', 'Y'],
    ['title', 'VARCHAR2', 'Y'],
    ['phone', 'VARCHAR2', 'Y'],
    ['type', 'VARCHAR2', 'Y'],
    ['favoriteFruit', 'VARCHAR2', 'Y'],
    ['age', 'NUMBER', 'Y']
  ],
  resultSet: undefined,
  outBinds: undefined,
  rowsAffected: undefined,
  metaData: [{
    name: 'COLUMN_NAME'
  }, {
    name: 'DATA_TYPE'
  }, {
    name: 'NULLABLE'
  }]
}

describe.skip('util tests', function() {
  it('should transform select results into a schema', (done) => {
      const norm = utils.normalizeSchema(results)
      norm.id.type.should.equal('NUMBER')
      done()
  })

  it('should use the correct parameter symbol', (done) => {
    const waterlineSequelQuery = 'INSERT INTO "test_create" (field_1, field_2) values ($1, $2)'
    const oracleQuery = utils.alterParameterSymbol(waterlineSequelQuery, ':')
    oracleQuery.should.equal('INSERT INTO "test_create" (field_1, field_2) values (:1, :2)')
    done()
  })
})
