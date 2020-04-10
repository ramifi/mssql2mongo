// Write table name
const tableName = 'tablename';

// Write SQL connection string 
const sqlConfig = {
  user: 'user',
  password: '123456',
  server: '127.0.0.1',
  port: 1234,
  database: 'dbname',
  pool: {
    max: 10,
    min: 0,
    idleTimeoutMillis: 30000
  }
}

// Write mongodb connection string
const mongoUrl = 'mongodb://{user}:{password}}@{ip}:{port}';

// Write mongodb db name
const dbName = 'dbname';


import commandLineUsage = require('command-line-usage')

const sections = [
  {
    header: 'Transfer MS SQL tables/views to MongoDB collections',
    content: 'Generates something {italic very} important.'
  },
  {
    header: 'Options',
    optionList: [
      {
        name: 'input',
        typeLabel: '{underline file}',
        description: 'The input to process.'
      },
      {
        name: 'help',
        description: 'Print this usage guide.'
      }
    ]
  }
]
const usage = commandLineUsage(sections)
console.log(usage)


let tablePK = [];


const sql = require('mssql');
const MongoClient = require('mongodb').MongoClient;



const mongoClient = new MongoClient(mongoUrl);

mongoClient.connect(function (err, mongodbClient) {
  const mongoDB = mongodbClient.db(dbName);




  sql.connect(sqlConfig, err => {
    // ... error checks

    new sql.Request().query(`SELECT Col.Column_Name from 
  INFORMATION_SCHEMA.TABLE_CONSTRAINTS Tab, 
  INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE Col 
WHERE 
  Col.Constraint_Name = Tab.Constraint_Name
  AND Col.Table_Name = Tab.Table_Name
  AND Constraint_Type = 'PRIMARY KEY'
  AND Col.Table_Name = '${tableName}'`, (sqlErr, result) => {
      // ... error checks
      tablePK  = result.recordset;



      const request = new sql.Request()
      request.stream = true // You can set streaming differently for each request
      request.query(`select * from ${tableName}`) // or request.execute(procedure)

      request.on('recordset', columns => {
        // Emitted once for each recordset in a query
        console.log(`rocordset: ${columns}`);
      })

      request.on('row', row => {
        // Emitted for each row in a recordset

        if (tablePK.length ==1){
          row._id = row[tablePK[0].Column_Name];
        }

        mongoDB.collection(tableName).insertOne(row, function (err, r) {
          if (err) {
            console.log(`mongo error: ${err.message}`);

          }
          console.log(`row inserted to mongo: ${r}`);

        });

        console.log(`row: ${row}`);
      })

      request.on('error', err => {
        // May be emitted multiple times
        mongodbClient.close();

        console.log(`error: ${err}`);

      })

      request.on('done', result => {
        // Always emitted as the last one
        mongodbClient.close();

        console.log(`done: ${result}`);
      })
    })

    sql.on('error', err => {
      // ... error handler
    })

  })
  // mongodbClient.close();
});

