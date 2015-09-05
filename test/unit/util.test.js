import should from 'should'
import utils from '../../dist/utils'
const results = {
  rows: [{
    COLUMN_NAME: 'id',
    DATA_TYPE: 'NUMBER',
    NULLABLE: 'Y'
  }, {
    COLUMN_NAME: 'name',
    DATA_TYPE: 'VARCHAR2',
    NULLABLE: 'N'
  }, {
    COLUMN_NAME: 'email',
    DATA_TYPE: 'VARCHAR2',
    NULLABLE: 'Y'
  }, {
    COLUMN_NAME: 'title',
    DATA_TYPE: 'VARCHAR2',
    NULLABLE: 'Y'
  }, {
    COLUMN_NAME: 'phone',
    DATA_TYPE: 'VARCHAR2',
    NULLABLE: 'Y'
  }, {
    COLUMN_NAME: 'type',
    DATA_TYPE: 'VARCHAR2',
    NULLABLE: 'Y'
  }, {
    COLUMN_NAME: 'favoriteFruit',
    DATA_TYPE: 'VARCHAR2',
    NULLABLE: 'Y'
  }, {
    COLUMN_NAME: 'age',
    DATA_TYPE: 'NUMBER',
    NULLABLE: 'Y'
  }],
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

describe('util tests', function() {
  it('should transform select results into a schema', (done) => {
    const norm = utils.normalizeSchema(results, {})
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
