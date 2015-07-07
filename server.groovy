import groovy.sql.Sql
import groovy.json.JsonSlurper
import groovy.json.JsonBuilder

def cli = new CliBuilder( usage: 'driver_server.groovy')
cli.with {
    p longOpt:'port', required: true, args:1, argName:'port', 'Port to listen on '
    c longOpt:'config',required: true, args:1, argName:'config','Json file configuration settings'
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

/*
enum Commands {
    case 1: // DONE
    case 2: // prepare
    case 3: // setLong
    case 4: // setString
    case 5: // execute
    case 6: // next
    case 7: // get
    case 8: // setDouble
    case 9: // close statement 
    case 10: // close result set 
    case 11: // begin transaction
    case 12: // commit transaction
    case 13: // rollback transaction
    case 14: // set time
    case 15: // set null
}

enum GetTypes {
    case 1: // int
    case 2: // string
    case 3: // double
    case 4: // float
    case 5: // time
    case 6: // long
    case 7: // short
    case 8: // byte
    case 9: // boolean
    case 10: // big decimal
    case 11: // timestamp
}
*/

def processResult = {sock->
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
            connection.setTransactionIsolation(java.sql.Connection.TRANSACTION_SERIALIZABLE)


            writeString("d67c184ff3c42e7b7a0bf2d4bca50340");
            dataOut.flush();

            
            byte selector;
            while (true) {
                try {
                    selector = dataIn.readByte()
                    if(selector==-1) {
                        break;
                    }
                } catch(java.io.EOFException e) {
                    break;
                }
                try {
                    switch (selector) {
                    case 11: // begin transaction
                        connection.setAutoCommit(false);
                        break;

                    case 12: // commit transaction
                        connection.commit();
                        connection.setAutoCommit(true);
                        break;

                    case 13: // rollback transaction
                        connection.rollback();
                        connection.setAutoCommit(true);
                        break;

                    case 9: // close statement 
                        String id = readString();
                        java.sql.PreparedStatement s = stmts.get(id);
                        s.close();
                        stmts.remove(id);
                        break;

                    case 10: // close result set 
                        String id = readString();
                        java.sql.ResultSet rs = results.get(id);
                        if(rs) {
                            rs.close();
                            results.remove(id);
                            resultMeta.remove(id);
                        }
                        break;

                    case 2: // prepare
                        String id = readString();
                        String q = readString();
                        java.sql.PreparedStatement s = connection.prepareStatement(q);
                        stmts.put(id,s);
                        break;

                    case 3: // setLong
                        String id = readString();
                        int a = dataIn.readInt();
                        long b = dataIn.readLong();
                        stmts.get(id).setLong(a,b);
                        break;

                    case 4: // setString
                        String id = readString();
                        int a = dataIn.readInt();
                        String b = readString();
                        stmts.get(id).setString(a,b);
                        break;

                    case 8: // setDouble
                        String id = readString();
                        int a = dataIn.readInt();
                        double b = dataIn.readDouble();
                        stmts.get(id).setDouble(a,b);
                        break;

                    case 14: // set time
                        String id = readString();
                        int a = dataIn.readInt();
                        long b = dataIn.readLong();
                        stmts.get(id).setTimestamp(a,new java.sql.Timestamp(b));
                        break;
                    case 15:
                        String id = readString();
                        int a = dataIn.readInt();
                        stmts.get(id).setObject(a,null);
                        break;    
                        
                    case 5: // execute
                        String id = readString();
                        java.sql.PreparedStatement s = stmts.get(id);
                        try {
                            boolean r = s.execute();
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
                        
                    case 6: // next
                        int batchSize = dataIn.readInt();
                        String id = readString();
                        java.sql.ResultSet rs = results.get(id);
                        for(int row=0;row<batchSize;row++) {
                            if (rs.next()) {
                                dataOut.writeByte(1);
                            } else {
                                dataOut.writeByte(0);
                                break;
                            }

                            java.sql.ResultSetMetaData md = resultMeta.get(id);
                            int n = md.getColumnCount();
                            for (int i=1; i<=n; i++) {
                                switch (md.getColumnClassName(i)) {
                                case "java.lang.Integer":
                                    int val = rs.getInt(i);
                                    if (rs.wasNull()) {
                                        dataOut.writeByte(0);
                                        continue;
                                    }
                                    dataOut.writeByte(1);
                                    dataOut.writeInt(val);
                                    break;
                                    
                                case "java.lang.String":
                                    String val = rs.getString(i);
                                    if (rs.wasNull()) {
                                        dataOut.writeByte(0);
                                        continue;
                                    }
                                    dataOut.writeByte(1);
                                    writeString(val);
                                    break;
                                    
                                case "java.lang.Double":
                                    double val = rs.getDouble(i);
                                    if (rs.wasNull()) {
                                        dataOut.writeByte(0);
                                        continue;
                                    }
                                    dataOut.writeByte(1);
                                    dataOut.writeDouble(val);
                                    break;
                                    
                                case "java.lang.Float":
                                    float val = rs.getFloat(i);
                                    if (rs.wasNull()) {
                                        dataOut.writeByte(0);
                                        continue;
                                    }
                                    dataOut.writeByte(1);
                                    dataOut.writeFloat(val);
                                    break;
                                    
                                case "java.sql.Date":
                                    java.sql.Date val = rs.getDate(i);
                                    if (rs.wasNull()) {
                                        dataOut.writeByte(0);
                                        continue;
                                    }
                                    dataOut.writeByte(1);
                                    dataOut.writeLong(i.getTime());
                                    break;
                                    
                                case "java.sql.Timestamp":
                                    java.sql.Timestamp val = rs.getTimestamp(i);
                                    if (rs.wasNull()) {
                                        dataOut.writeByte(0);
                                        continue;
                                    }
                                    dataOut.writeByte(1);
                                    dataOut.writeLong(val.getTime());
                                    break;
                                    
                                case "java.lang.Long":
                                    long val = rs.getLong(i);
                                    if (rs.wasNull()) {
                                        dataOut.writeByte(0);
                                        continue;
                                    }
                                    dataOut.writeByte(1);
                                    dataOut.writeLong(val);
                                    break;
                                    
                                case "java.lang.Short":
                                    short val = rs.getShort(i);
                                    if (rs.wasNull()) {
                                        dataOut.writeByte(0);
                                        continue;
                                    }
                                    dataOut.writeByte(1);
                                    dataOut.writeShort(val);
                                    break;
                                    
                                case "java.lang.Byte":
                                    byte val = rs.getByte(i);
                                    if (rs.wasNull()) {
                                        dataOut.writeByte(0);
                                        continue;
                                    }
                                    dataOut.writeByte(1);
                                    dataOut.writeByte(val);
                                    break;
                                    
                                case "java.lang.Boolean":
                                    boolean val = rs.getBoolean(i);
                                    if (rs.wasNull()) {
                                        dataOut.writeByte(0);
                                        continue;
                                    }
                                    dataOut.writeByte(1);
                                    dataOut.writeByte(val ? 1 : 0);
                                    break;
                                    
                                case "java.math.BigDecimal":
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
                connection.close()
            }
            if(stmts) {
                try {
                    stmts.each {k,v->v.close()}
                } catch(e) {
                }
            }
            if(results) {
                try {
                    results.each {k,v->v.close()}
                } catch(e) {
                }
            }
            

        }
    }
    try {
        sock.close()
    } catch (e){
        e.printStackTrace()
    }
}

while (true) {
    def socket = server.accept(true,processResult)
}
