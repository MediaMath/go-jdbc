import groovy.sql.Sql
import groovy.json.JsonSlurper
import groovy.json.JsonBuilder
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.ConcurrentHashMap
import groovyx.gpars.GParsPool

def cli = new CliBuilder( usage: 'server.groovy')
cli.with {
    p longOpt:'port', required: true, args:1, argName:'port', 'Port to listen on'
    c longOpt:'config',required: true, args:1, argName:'config','JSON file configuration settings'
    t longOpt:'transaction-level',args:1, argName:'transaction-level', 'The JDBC transaction level to use for all connections.'
    o longOpt:'override-timeout',args:1, argName:'override-timeout','Use an additional java level timeout to circumvent timeout bugs in different driver implementations.'
}

def myOptions = cli.parse(args)


if(!myOptions) {
    System.exit(1)
} else if (!myOptions.p && !myOptions.c) {
    cli.usage()
    System.exit(1)
}

def config = new JsonSlurper().parse(new File(myOptions.c))
if(config.driver) {
Class.forName(config.driver)
}

def server = new ServerSocket(myOptions.p.toInteger())

// Commands
byte commandDone = 1
byte commandPrepare = 2
byte commandSetLong = 3
byte commandSetString = 4
byte commandExecute = 5
byte commandNext = 6
byte commandGet = 7
byte commandSetDouble = 8
byte commandCloseStatement  = 9
byte commandCloseResultSet  = 10
byte commandBeginTransaction = 11
byte commandCommitTransaction = 12
byte commandRollbackTransaction = 13
byte commandSetTime = 14
byte commandSetNull = 15
byte commandSetQueryTimeout = 16

byte commandCloseConnection = -1
byte commandServerStatus = -2

def concurrentRequests = new AtomicInteger()
def connectionsInLastHour = new AtomicInteger()


Long overrideTimeoutLength = 0L;
if(myOptions.o && myOptions.o?.isLong()) {
    overrideTimeoutLength = (Long)myOptions.o.toLong();
}

def processResult = {sock->
    concurrentRequests.getAndAdd(1);
    sock.withStreams {inputStream,outputStream->
        java.sql.Connection connection;
        def stmts = [:]
        def results = [:]
        def resultMeta = [:]
        try {
            DataInputStream dataIn = new DataInputStream(inputStream);
            DataOutputStream dataOut = new DataOutputStream(outputStream);

            def writeString = {String s-> 
                if (s==null) {
                    s = "";
                }
                byte[] buf = s.getBytes("UTF-8");
                dataOut.writeInt(buf.length);
                dataOut.write(buf);
            }

            def readString = {
                int len = dataIn.readInt();
                byte[] buf = new byte[len];
                try {
                    dataIn.readFully(buf);
                } catch(e) {
                }
                return new String(buf,"UTF-8");
            }

            // Connect to database
            connection = java.sql.DriverManager.getConnection(config.url,config.user,config.password)
            connection.setAutoCommit(true)
            switch(myOptions.t) {
                case "NONE":
                    connection.setTransactionIsolation(java.sql.Connection.TRANSACTION_NONE)
                    break
                case "READ_COMMITTED":
                    connection.setTransactionIsolation(java.sql.Connection.TRANSACTION_READ_COMMITTED)
                    break
                case "READ_UNCOMMITTED":
                    connection.setTransactionIsolation(java.sql.Connection.TRANSACTION_READ_UNCOMMITTED)
                    break
                case "REPEATABLE_READ":
                    connection.setTransactionIsolation(java.sql.Connection.TRANSACTION_REPEATABLE_READ)
                    break
                case "SERIALIZABLE":
                    connection.setTransactionIsolation(java.sql.Connection.TRANSACTION_SERIALIZABLE)
                    break
            }

            writeString("d67c184ff3c42e7b7a0bf2d4bca50340");
            dataOut.flush();

            byte selector;
            while (true) {
                try {
                    selector = dataIn.readByte()
                    if(selector<0) {
                        switch(selector){
                            case commandServerStatus:
                                writeString("""Concurrent Requests (including this): ${concurrentRequests.intValue()}
Connections in the last hour: ${connectionsInLastHour.intValue()}""");
                                break;
                            case commandCloseConnection:
                                println "Close connection received.";
                        }
                        break;
                    }
                } catch(java.io.EOFException e) {
                    break;
                }

                try {
                    switch (selector) {
                    case commandBeginTransaction:
                        connection.setAutoCommit(false);
                        break;

                    case commandCommitTransaction:
                        try {
                            stmts.each {i,s->
                                if(!s.hasBatch) {return}
                                s.s.executeBatch();
                            }
                            connection.commit();
                            dataOut.writeByte(0);
                        } catch (e) {
                            dataOut.writeByte(1);
                            writeString(e.getMessage()?:e.toString() );
                        }
                        connection.setAutoCommit(true);
                        break;

                    case commandRollbackTransaction:
                        try {
                            connection.rollback();
                            dataOut.writeByte(0);
                        } catch (e) {
                            dataOut.writeByte(1);
                            writeString(e.getMessage());
                        }
                        connection.setAutoCommit(true);
                        break;

                    case commandCloseStatement:
                        String id = readString();
                        def myStatment = stmts.get(id);
                        java.sql.PreparedStatement s = myStatment.s;
                        try {
                            if(myStatment.hasBatch) {
                                s.executeBatch();
                            }
                        } catch (e) {
                        }

                        try {
                            s.close();
                            dataOut.writeByte(0);
                        } catch (e) {
                            dataOut.writeByte(1);
                            writeString(e.getMessage());
                        } finally {
                            stmts.remove(id);
                        }
                        break;

                    case commandCloseResultSet:
                        String id = readString();
                        java.sql.ResultSet rs = results.get(id);
                        if(rs) {
                            rs.close();
                            results.remove(id);
                            resultMeta.remove(id);
                        }
                        break;

                    case commandPrepare:
                        String id = readString();
                        String q = readString();
                        
                        try {
                            java.sql.PreparedStatement s = connection.prepareStatement(q);
                            def myStatment = ["s":s];
                            myStatment.insertUpdate = (q.toLowerCase() =~ /^(insert|update).*/).matches();
                            stmts.put(id,myStatment);
                            dataOut.writeByte(0);
                        } catch(java.sql.SQLSyntaxErrorException e) {
                            dataOut.writeByte(1);
                            writeString(e.getMessage()?:e.toString());
                        }
                        break;

                    case commandSetQueryTimeout:
                        String id = readString();
                        int a = dataIn.readLong();
                        def myStatment = stmts.get(id);
                        try {
                            java.sql.PreparedStatement s = myStatment.s;
                            s.setQueryTimeout(a);
                            dataOut.writeByte(0);
                        } catch (java.sql.SQLException e) {
                            dataOut.writeByte(1);
                            writeString(e.getMessage());
                        }
                        break;

                    case commandSetLong:
                        String id = readString();
                        int a = dataIn.readInt();
                        long b = dataIn.readLong();
                        stmts.get(id).s.setLong(a,b);
                        break;

                    case commandSetString:
                        String id = readString();
                        int a = dataIn.readInt();
                        String b = readString();
                        stmts.get(id).s.setString(a,b);
                        break;

                    case commandSetDouble:
                        String id = readString();
                        int a = dataIn.readInt();
                        double b = dataIn.readDouble();
                        stmts.get(id).s.setDouble(a,b);
                        break;

                    case commandSetTime:
                        String id = readString();
                        int a = dataIn.readInt();
                        long b = dataIn.readLong();
                        stmts.get(id).s.setTimestamp(a,new java.sql.Timestamp(b));
                        break;
                    case commandSetNull:
                        String id = readString();
                        int a = dataIn.readInt();
                        stmts.get(id).s.setObject(a,null);
                        break;    
                        
                    case commandExecute:
                        String id = readString();
                        def myStatement = stmts.get(id);
                        java.sql.PreparedStatement s = myStatement.s;

                        if(!connection.getAutoCommit() && myStatement.insertUpdate) {
                            try {
                                s.addBatch();
                                myStatement["hasBatch"] = true
                                stmts.put(id,myStatement);
                                dataOut.writeByte(0);
                            } catch (e) {
                                dataOut.writeByte(2);
                                writeString(e.getMessage());
                            }
                            break;
                        }

                        try {
                            boolean r = false;
                            if(overrideTimeoutLength>0) {
                                try {
                                    GParsPool.withPool {
                                        def execer = {s.execute()}.async();
                                        r = execer().get(overrideTimeoutLength,java.util.concurrent.TimeUnit.SECONDS);
                                    }
                                } catch(java.util.concurrent.ExecutionException e) {
                                    throw e.cause
                                }
                            } else {
                                r = s.execute();
                            }

                            if (r) {
                                dataOut.writeByte(1);
                                dataOut.flush(); // need to flush here, due to round-trip in this protocol :-(
                                java.sql.ResultSet rs = s.getResultSet();
                                rs.setFetchSize(1000);
                                String id2 = readString();
                                results.put(id2,rs);
                                java.sql.ResultSetMetaData md = rs.getMetaData();
                                resultMeta.put(id2,md);
                                int n = md.getColumnCount();
                                dataOut.writeInt(n);
                                for (int i=0; i<n; i++) {
                                    writeString(md.getColumnName(i+1));
                                    writeString(md.getColumnClassName(i+1));
                                }
                            } else {
                                dataOut.writeByte(0);
                                int c = s.getUpdateCount();
                                dataOut.writeInt(c);
                            }
                        } catch (java.sql.SQLException e) {
                            dataOut.writeByte(2);
                            writeString(e.getMessage());
                        }
                        break;
                        
                    case commandNext:
                        int batchSize = dataIn.readInt();
                        String id = readString();
                        java.sql.ResultSet rs = results.get(id);
                        for(int row=0;row<batchSize;row++) {
                            boolean nextResult = false;
                            if(overrideTimeoutLength>0) {
                                try {
                                    GParsPool.withPool {
                                        def execer = {rs.next()}.async();
                                        nextResult = execer().get(overrideTimeoutLength,java.util.concurrent.TimeUnit.SECONDS);
                                    }
                                } catch(java.util.concurrent.ExecutionException e) {
                                    throw e.cause
                                }
                            } else {
                                nextResult = rs.next();
                            }

                            if (nextResult) {
                                dataOut.writeByte(1);
                            } else {
                                dataOut.writeByte(0);
                                break;
                            }

                            java.sql.ResultSetMetaData md = resultMeta.get(id);
                            int n = md.getColumnCount();
                            for (int i=1; i<=n; i++) {
                                switch (md.getColumnClassName(i)) {
                                case java.lang.Integer.getName():
                                    int val = rs.getInt(i);
                                    if (rs.wasNull()) {
                                        dataOut.writeByte(0);
                                        continue;
                                    }
                                    dataOut.writeByte(1);
                                    dataOut.writeInt(val);
                                    break;
                                    
                                case java.lang.String.getName():
                                    String val = rs.getString(i);
                                    if (rs.wasNull()) {
                                        dataOut.writeByte(0);
                                        continue;
                                    }
                                    dataOut.writeByte(1);
                                    writeString(val);
                                    break;
                                    
                                case java.lang.Double.getName():
                                    double val = rs.getDouble(i);
                                    if (rs.wasNull()) {
                                        dataOut.writeByte(0);
                                        continue;
                                    }
                                    dataOut.writeByte(1);
                                    dataOut.writeDouble(val);
                                    break;
                                    
                                case java.lang.Float.getName():
                                    float val = rs.getFloat(i);
                                    if (rs.wasNull()) {
                                        dataOut.writeByte(0);
                                        continue;
                                    }
                                    dataOut.writeByte(1);
                                    dataOut.writeFloat(val);
                                    break;
                                    
                                case java.sql.Date.getName():
                                    java.sql.Date val = rs.getDate(i);
                                    if (rs.wasNull()) {
                                        dataOut.writeByte(0);
                                        continue;
                                    }
                                    dataOut.writeByte(1);
                                    dataOut.writeLong(i.getTime());
                                    break;
                                    
                                case java.sql.Timestamp.getName():
                                    java.sql.Timestamp val = rs.getTimestamp(i);
                                    if (rs.wasNull()) {
                                        dataOut.writeByte(0);
                                        continue;
                                    }
                                    dataOut.writeByte(1);
                                    dataOut.writeLong(val.getTime());
                                    break;
                                    
                                case java.lang.Long.getName():
                                    long val = rs.getLong(i);
                                    if (rs.wasNull()) {
                                        dataOut.writeByte(0);
                                        continue;
                                    }
                                    dataOut.writeByte(1);
                                    dataOut.writeLong(val);
                                    break;
                                    
                                case java.lang.Short.getName():
                                    short val = rs.getShort(i);
                                    if (rs.wasNull()) {
                                        dataOut.writeByte(0);
                                        continue;
                                    }
                                    dataOut.writeByte(1);
                                    dataOut.writeShort(val);
                                    break;
                                    
                                case java.lang.Byte.getName():
                                    byte val = rs.getByte(i);
                                    if (rs.wasNull()) {
                                        dataOut.writeByte(0);
                                        continue;
                                    }
                                    dataOut.writeByte(1);
                                    dataOut.writeByte(val);
                                    break;
                                    
                                case java.lang.Boolean.getName():
                                    boolean val = rs.getBoolean(i);
                                    if (rs.wasNull()) {
                                        dataOut.writeByte(0);
                                        continue;
                                    }
                                    dataOut.writeByte(1);
                                    dataOut.writeByte(val ? 1 : 0);
                                    break;
                                    
                                case java.math.BigDecimal.getName():
                                    java.math.BigDecimal val = rs.getBigDecimal(i);
                                    if (rs.wasNull()) {
                                        dataOut.writeByte(0);
                                        continue;
                                    }
                                    dataOut.writeByte(1);
                                    writeString(val.toString());
                                    break;
                                }
                            }
                        }
                        break;

                    default:
                        throw new Exception("java unknown byte: " + selector);
                    }
                } finally {
                    dataOut.flush();
                } // End try
            } // End while
            
        } catch(e) {
            e.printStackTrace()
        }finally {
            // Flush and close outputs and database connection
            try {
                outputStream.close()
            } catch(e) {
                println "Error closing ${e}"
            }

            if(connection) {
                if(results) {
                    println "Clearing out resources: ${results.size()} results."
                    results.each {k,v->
                        try {
                            v.s.close()
                        } catch(e) {
                            println "Error closing ${e}"
                        }
                    }
                }

                if(stmts) {
                    println "Clearing out resources: ${stmts.size()} statements."
                    stmts.each {k,v->
                        try {
                            v.s.close()
                        } catch(e) {
                            println "Error closing ${e}"
                        }
                    }
                }
                connection.close();
            }
        }
    }
    try {
        sock.close()
    } catch (e){
        e.printStackTrace()
    } finally {
        concurrentRequests.getAndAdd(-1);
    }
}

def connectionTracker = []
while (true) {

    // Remove any 10 minutes old
    connectionTracker.removeAll {it.time < (new Date().time-(1000*60*60))}

    // Add another to the recent ones
    connectionTracker.push(new Date())

    connectionsInLastHour.set(connectionTracker.size())

    def socket = server.accept(true,processResult)
}
