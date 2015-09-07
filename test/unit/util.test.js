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
const outbinds = [
  ['update_user2',
    'update_user8',
    'update_user3',
    'update_user5',
    'update_user7',
    'update_user9',
    'update_user1',
    'update_user4',
    'update_user0',
    'update_user6'
  ],
  ['updated',
    'updated',
    'updated',
    'updated',
    'updated',
    'updated',
    'updated',
    'updated',
    'updated',
    'updated'
  ],
  [null, null, null, null, null, null, null, null, null, null],
  [null, null, null, null, null, null, null, null, null, null],
  [null, null, null, null, null, null, null, null, null, null],
  ['update',
    'update',
    'update',
    'update',
    'update',
    'update',
    'update',
    'update',
    'update',
    'update'
  ],
  ['blueberry',
    'blueberry',
    'blueberry',
    'blueberry',
    'blueberry',
    'blueberry',
    'blueberry',
    'blueberry',
    'blueberry',
    'blueberry'
  ],
  [null, null, null, null, null, null, null, null, null, null],
  [null, null, null, null, null, null, null, null, null, null],
  [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
  [null, null, null, null, null, null, null, null, null, null],
  [null, null, null, null, null, null, null, null, null, null],
  [81, 87, 82, 84, 85, 88, 80, 83, 79, 86],
  ['06-SEP-15 02.38.29.578000 PM',
    '06-SEP-15 02.38.29.579000 PM',
    '06-SEP-15 02.38.29.578000 PM',
    '06-SEP-15 02.38.29.578000 PM',
    '06-SEP-15 02.38.29.579000 PM',
    '06-SEP-15 02.38.29.579000 PM',
    '06-SEP-15 02.38.29.577000 PM',
    '06-SEP-15 02.38.29.578000 PM',
    '06-SEP-15 02.38.29.577000 PM',
    '06-SEP-15 02.38.29.579000 PM'
  ],
  ['06-SEP-15 02.38.29.621000 PM',
    '06-SEP-15 02.38.29.621000 PM',
    '06-SEP-15 02.38.29.621000 PM',
    '06-SEP-15 02.38.29.621000 PM',
    '06-SEP-15 02.38.29.621000 PM',
    '06-SEP-15 02.38.29.621000 PM',
    '06-SEP-15 02.38.29.621000 PM',
    '06-SEP-15 02.38.29.621000 PM',
    '06-SEP-15 02.38.29.621000 PM',
    '06-SEP-15 02.38.29.621000 PM'
  ],
  [null, null, null, null, null, null, null, null, null, null],
  [null, null, null, null, null, null, null, null, null, null]
]
const outbindFields = ['"first_name"',
  '"last_name"',
  '"avatar"',
  '"title"',
  '"phone"',
  '"type"',
  '"favoriteFruit"',
  '"age"',
  '"dob"',
  '"status"',
  '"percent"',
  '"obj"',
  '"id"',
  '"createdAt"',
  '"updatedAt"',
  '"emailAddress"',
  '"arrList"'
]

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

  it('should transform outbinds', (done) => {
    const results = utils.transformBulkOutbinds(outbinds, outbindFields)
    results.length.should.equal(outbinds[0].length)
    done()
  })
})
