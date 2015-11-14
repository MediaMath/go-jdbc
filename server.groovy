import groovy.sql.Sql
import groovy.json.JsonSlurper
import groovy.json.JsonBuilder
import java.util.concurrent.atomic.AtomicInteger

def cli = new CliBuilder( usage: 'server.groovy')
cli.with {
    p longOpt:'port', required: true, args:1, argName:'port', 'Port to listen on'
    c longOpt:'config',required: true, args:1, argName:'config','JSON file configuration settings'
    t longOpt:'transaction-level',args:1, argName:'transaction-level', 'The JDBC transaction level to use for all connections.'
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

byte responseSuccess = 17
byte responseFetchError = 18
byte responseNull = 19
byte responseFetchNoMore = 20
byte responseTrue = 21
byte responseFalse = 22
byte responseNotNull = 23
byte responseFetchHasResults = 24
byte responseCommitSuccess = 25
byte responseCommitError = 26
byte responseRollbackSuccess = 27
byte responseRollbackError = 28
byte responseCloseStatementSuccess = 29
byte responseCloseStatementError = 30
byte responsePrepareSuccess = 31
byte responsePrepareError = 32
byte responseSetQueryTimeoutSuccess = 33
byte responseSetQueryTimeoutError = 34
byte responseBatchAddSuccess = 35
byte responseBatchAddError = 36
byte responseQueryHasResults = 37
byte responseQueryNoResults = 38
byte responseExecError = 39
byte commandQuery = 40
byte responseQueryError = 41
byte responseExecHasUnexpectedResults = 42
byte responseExecSuccess = 43

def concurrentRequests = new AtomicInteger()
def connectionsInLastHour = new AtomicInteger()


def logMessage = {aMessage->
    println "${new Date().format('YYYY/MM/DD hh:mm:ss')} ${aMessage}"
}



def readString = {DataInputStream input->
    int len = input.readInt();
    byte[] buf = new byte[len];
    input.readFully(buf);
    return new String(buf,"UTF-8");
}

def writeString = {String s, DataOutputStream out->
    if (s==null) {
        s = "";
    }
    byte[] buf = s.getBytes("UTF-8");
    out.writeInt(buf.length);
    out.write(buf);
}


def processNext = {java.sql.ResultSetMetaData md, java.sql.ResultSet rs, int aFetchSize, DataOutputStream out ->
    def wasNull = {
        if(rs.wasNull()) {
            out.writeByte(responseNull);
            return true;
        }
        out.writeByte(responseNotNull);
        return false;
    }
    for(int row=0;row<aFetchSize;row++) {
        boolean nextResult = false;
        try {
            nextResult = rs.next();

            if (nextResult) {
                out.writeByte(responseFetchHasResults);
            } else {
                out.writeByte(responseFetchNoMore);
                break;
            }

        } catch (java.sql.SQLException e) {
            out.writeByte(responseFetchError);
            writeString(e.getMessage(),out);
            break;
        }

        int n = md.getColumnCount();
        for (int i=1; i<=n; i++) {
            switch (md.getColumnClassName(i)) {
            case java.lang.Integer.getName():
                int val = rs.getInt(i);
                if (wasNull()) {continue;}
                out.writeInt(val);
                break;
                
            case java.lang.String.getName():
                String val = rs.getString(i);
                if (wasNull()) {continue;}
                writeString(val,out);
                break;
                
            case java.lang.Double.getName():
                double val = rs.getDouble(i);
                if (wasNull()) {continue;}
                out.writeDouble(val);
                break;
                
            case java.lang.Float.getName():
                float val = rs.getFloat(i);
                if (wasNull()) {continue;}
                out.writeFloat(val);
                break;
                
            case java.sql.Date.getName():
                java.sql.Date val = rs.getDate(i);
                if (wasNull()) {continue;}
                out.writeLong(i.getTime());
                break;
                
            case java.sql.Timestamp.getName():
                java.sql.Timestamp val = rs.getTimestamp(i);
                if (wasNull()) {continue;}
                out.writeLong(val.getTime());
                break;
                
            case java.lang.Long.getName():
                long val = rs.getLong(i);
                if (wasNull()) {continue;}
                out.writeLong(val);
                break;
                
            case java.lang.Short.getName():
                short val = rs.getShort(i);
                if (wasNull()) {continue;}
                out.writeShort(val);
                break;
                
            case java.lang.Byte.getName():
                byte val = rs.getByte(i);
                if (wasNull()) {continue;}
                out.writeByte(val);
                break;
                
            case java.lang.Boolean.getName():
                boolean val = rs.getBoolean(i);
                if (wasNull()) {continue;}
                out.writeByte(val ? responseTrue : responseFalse);
                break;
                
            case java.math.BigDecimal.getName():
                java.math.BigDecimal val = rs.getBigDecimal(i);
                if (wasNull()) {continue;}
                writeString(val.toString(),out);
                break;
            }
        }
    }
}

def processResult = {sock->
    concurrentRequests.getAndAdd(1);
    sock.withStreams {inputStream,outputStream->
        java.sql.Connection connection;
        def stmts = [:]
        def results = [:]
        def resultMeta = [:]
        def stmtsFetchSize = [:]
        def resultsFetchSize = [:]
        try {
            DataInputStream dataIn = new DataInputStream(inputStream);
            DataOutputStream dataOut = new DataOutputStream(outputStream);

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

            writeString("d67c184ff3c42e7b7a0bf2d4bca50340",dataOut);
            dataOut.flush();

            byte selector;
            while (true) {
                try {
                    selector = dataIn.readByte()
                    if(selector<0) {
                        switch(selector){
                            case commandServerStatus:
                                writeString("""Concurrent Requests (including this): ${concurrentRequests.intValue()}
Connections in the last hour: ${connectionsInLastHour.intValue()}""",dataOut);
                                break;
                            case commandCloseConnection:
                                logMessage "Close connection received.";
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
                            dataOut.writeByte(responseCommitSuccess);
                        } catch (e) {
                            dataOut.writeByte(responseCommitError);
                            writeString(e.getMessage()?:e.toString() );
                        }
                        connection.setAutoCommit(true);
                        break;

                    case commandRollbackTransaction:
                        try {
                            connection.rollback();
                            dataOut.writeByte(responseRollbackSuccess);
                        } catch (e) {
                            dataOut.writeByte(responseRollbackError);
                            writeString(e.getMessage());
                        }
                        connection.setAutoCommit(true);
                        break;

                    case commandCloseStatement:
                        String id = readString(dataIn);
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
                            dataOut.writeByte(responseCloseStatementSuccess);
                        } catch (e) {
                            dataOut.writeByte(responseCloseStatementError);
                            writeString(e.getMessage());
                        } finally {
                            stmts.remove(id);
                            stmtsFetchSize.remove(id);
                        }
                        break;

                    case commandCloseResultSet:
                        String id = readString(dataIn);
                        java.sql.ResultSet rs = results.get(id);

                        if(rs) {
                            rs.close();
                            results.remove(id);
                            resultMeta.remove(id);
                            resultsFetchSize.remove(id);
                        }
                        break;

                    case commandPrepare:
                        String id = readString(dataIn);
                        String q = readString(dataIn);
                        // Get the fetch size
                        int fetchSize = dataIn.readInt();
                        
                        // getFetchSize may return 0, this will not work got calls to next()
                        stmtsFetchSize[id] = fetchSize
                        
                        try {
                            java.sql.PreparedStatement s = connection.prepareStatement(q);
                            s.setFetchSize(fetchSize);
                            def myStatment = ["s":s];
                            myStatment.insertUpdate = (q.toLowerCase() =~ /^(insert|update).*/).matches();
                            stmts.put(id,myStatment);
                            dataOut.writeByte(responsePrepareSuccess);
                        } catch(java.sql.SQLSyntaxErrorException e) {
                            dataOut.writeByte(responsePrepareError);
                            writeString(e.getMessage()?:e.toString());
                        }
                        break;

                    case commandSetQueryTimeout:
                        String id = readString(dataIn);
                        int a = dataIn.readLong();
                        def myStatment = stmts.get(id);
                        try {
                            java.sql.PreparedStatement s = myStatment.s;
                            s.setQueryTimeout(a);
                            dataOut.writeByte(responseSetQueryTimeoutSuccess);
                        } catch (java.sql.SQLException e) {
                            dataOut.writeByte(responseSetQueryTimeoutError);
                            writeString(e.getMessage());
                        }
                        break;

                    case commandSetLong:
                        String id = readString(dataIn);
                        int a = dataIn.readInt();
                        long b = dataIn.readLong();
                        stmts.get(id).s.setLong(a,b);
                        break;

                    case commandSetString:
                        String id = readString(dataIn);
                        int a = dataIn.readInt();
                        String b = readString(dataIn);
                        stmts.get(id).s.setString(a,b);
                        break;

                    case commandSetDouble:
                        String id = readString(dataIn);
                        int a = dataIn.readInt();
                        double b = dataIn.readDouble();
                        stmts.get(id).s.setDouble(a,b);
                        break;

                    case commandSetTime:
                        String id = readString(dataIn);
                        int a = dataIn.readInt();
                        long b = dataIn.readLong();
                        stmts.get(id).s.setTimestamp(a,new java.sql.Timestamp(b));
                        break;
                    case commandSetNull:
                        String id = readString(dataIn);
                        int a = dataIn.readInt();
                        stmts.get(id).s.setObject(a,null);
                        break;

                    case commandQuery:
                        String id = readString(dataIn);
                        def myStatement = stmts.get(id);
                        java.sql.PreparedStatement s = myStatement.s;

                        try {
                            boolean r = s.execute();

                            if (r) {
                                dataOut.writeByte(responseQueryHasResults);
                                dataOut.flush(); // need to flush here, due to round-trip in this protocol
                                java.sql.ResultSet rs = s.getResultSet();

                                String id2 = readString(dataIn);
                                resultsFetchSize[id2]=stmtsFetchSize[id];
                                results.put(id2,rs);

                                // Send all column data over
                                java.sql.ResultSetMetaData md = rs.getMetaData();
                                resultMeta.put(id2,md);
                                int n = md.getColumnCount();
                                dataOut.writeInt(n);
                                for (int i=0; i<n; i++) {
                                    writeString(md.getColumnName(i+1),dataOut);
                                    writeString(md.getColumnClassName(i+1),dataOut);
                                }
                            } else {
                                dataOut.writeByte(responseQueryNoResults);
                            }
                        } catch (java.sql.SQLException e) {
                            dataOut.writeByte(responseQueryError);
                            writeString(e.getMessage(),dataOut);
                        }
                        break;
                        
                    case commandExecute:
                        String id = readString(dataIn);
                        def myStatement = stmts.get(id);
                        java.sql.PreparedStatement s = myStatement.s;

                        if(!connection.getAutoCommit()) {
                            try {
                                s.addBatch();
                                myStatement["hasBatch"] = true
                                stmts.put(id,myStatement);
                                dataOut.writeByte(responseBatchAddSuccess);
                            } catch (e) {
                                dataOut.writeByte(responseBatchAddError);
                                writeString(e.getMessage());
                            }
                        } else {
                            try {
                                boolean r = s.execute();

                                if (r) {
                                    dataOut.writeByte(responseExecHasUnexpectedResults);
                                } else {
                                    dataOut.writeByte(responseExecSuccess);
                                    int c = s.getUpdateCount();
                                    dataOut.writeInt(c);
                                }
                            } catch (java.sql.SQLException e) {
                                dataOut.writeByte(responseExecError);
                                writeString(e.getMessage(),dataOut);
                            }
                        }
                        break;
                        
                    case commandNext:
                        String id = readString(dataIn);
                        processNext(
                            resultMeta.get(id),
                            results.get(id),
                            resultsFetchSize[id],
                            dataOut
                        )
                        break;

                    default:
                        throw new Exception("java unknown byte: " + selector);
                    }
                } finally {
                    dataOut.flush();
                } // End try
            } // End while
            
        } catch(e) {
            logMessage "Caught exception:"
            e.printStackTrace()
        } finally {
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
                            v.close()
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
        logMessage "Caught exception:"
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