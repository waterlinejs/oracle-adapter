A basic CRUD waterline adapter for oracle, built on top of oracle's [driver](https://github.com/oracle/node-oracledb).
Most of this code is based on atiertant's [sails-oracle](https://github.com/atiertant/sails-oracle).

####Connection options (default values provided if they exist):

**connectString**:   
String - Required  

**user**:  
String - Required  

**password**:   
String - Required  

**poolMin**:   
Number - The minimum number of connections a connection pool maintains. Defaults to 0.  

**poolMax**:   
Number - The maximum number of connections to which a connection pool can grow. Defaults to 4.  

**poolIncrement**:   
Number - The number of connections that are opened when a connection request exceeds the number
of currently open connections. Defaults to 1.  

**poolTimeout**:    
Number - The number, in seconds, after which idle connections are terminated.  If set to 0, idle connections
are never terminated. Defaults to 60.  


##### Example connection (from test config)
    oracleConn: {  
        connectString: '192.168.201.100/xe',  
        user: 'myusername',  
        password: 'mypassword'  
    }

#####Running the tests  
To run the tests, supply the required env vars in `test/unit/support/bootstrap.js` for your test oracle instance.
