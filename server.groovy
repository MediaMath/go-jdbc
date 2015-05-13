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
                dataIn.readFully(buf);
                return new String(buf,"UTF-8");
            }

            // Connect to database
            java.sql.Connection connection = java.sql.DriverManager.getConnection(config.url,config.user,config.password)
            connection.setAutoCommit(true)
            connection.setTransactionIsolation(java.sql.Connection.TRANSACTION_SERIALIZABLE)

            def stmts = [:]
            def results = [:]

            writeString("d67c184ff3c42e7b7a0bf2d4bca50340");
            dataOut.flush();

            boolean done = false;
            while (!done) {
                try {
                    byte selector = dataIn.readByte();
                    switch (selector) {
                    case 1: // DONE
                        done = true;
                        dataOut.writeByte(1);
                        break;

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
                        rs.close();
                        results.remove(id);
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
                        
                    case 5: // execute
                        String id = readString();
                        java.sql.PreparedStatement s = stmts.get(id);
                        try {
                            boolean r = s.execute();
                            if (r) {
                            dataOut.writeByte(1);
                            dataOut.flush(); // need to flush here, due to round-trip in this protocol :-(
                            java.sql.ResultSet rs = s.getResultSet();
                            String id2 = readString();
                            results.put(id2,rs);
                            java.sql.ResultSetMetaData md = rs.getMetaData();
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
                        String id = readString();
                        java.sql.ResultSet rs = results.get(id);
                        if (rs.next()) {
                            dataOut.writeByte(1);
                        } else {
                            dataOut.writeByte(0);
                        }
                        break;
                        
                    case 7: // get
                        String id = readString();
                        int ind = dataIn.readInt();
                        java.sql.ResultSet rs = results.get(id);
                        switch (dataIn.readByte()) {
                        case 1: // int
                            int i = rs.getInt(ind);
                            if (rs.wasNull()) {
                                dataOut.writeByte(0);
                                continue;
                            }
                            dataOut.writeByte(1);
                            dataOut.writeInt(i);
                            break;
                            
                        case 2: // string
                            String i = rs.getString(ind);
                            if (rs.wasNull()) {
                                dataOut.writeByte(0);
                                continue;
                            }
                            dataOut.writeByte(1);
                            writeString(i);
                            break;
                            
                        case 3: // double
                            double i = rs.getDouble(ind);
                            if (rs.wasNull()) {
                                dataOut.writeByte(0);
                                continue;
                            }
                            dataOut.writeByte(1);
                            dataOut.writeDouble(i);
                            break;
                            
                        case 4: // float
                            float i = rs.getFloat(ind);
                            if (rs.wasNull()) {
                                dataOut.writeByte(0);
                                continue;
                            }
                            dataOut.writeByte(1);
                            dataOut.writeFloat(i);
                            break;
                            
                        case 5: // time
                            java.sql.Date i = rs.getDate(ind);
                            if (rs.wasNull()) {
                                dataOut.writeByte(0);
                                continue;
                            }
                            dataOut.writeByte(1);
                            dataOut.writeLong(i.getTime());
                            break;
                            
                        case 11: // timestamp
                            java.sql.Timestamp i = rs.getTimestamp(ind);
                            if (rs.wasNull()) {
                                dataOut.writeByte(0);
                                continue;
                            }
                            dataOut.writeByte(1);
                            dataOut.writeLong(i.getTime());
                            break;
                            
                            case 6: // long
                            long i = rs.getLong(ind);
                            if (rs.wasNull()) {
                                dataOut.writeByte(0);
                                continue;
                            }
                            dataOut.writeByte(1);
                            dataOut.writeLong(i);
                            break;
                            
                        case 7: // short
                            short i = rs.getShort(ind);
                            if (rs.wasNull()) {
                                dataOut.writeByte(0);
                                continue;
                            }
                            dataOut.writeByte(1);
                            dataOut.writeShort(i);
                            break;
                            
                        case 8: // byte
                            byte i = rs.getByte(ind);
                            if (rs.wasNull()) {
                                dataOut.writeByte(0);
                                continue;
                            }
                            dataOut.writeByte(1);
                            dataOut.writeByte(i);
                            break;
                            
                        case 9: // boolean
                            boolean i = rs.getBoolean(ind);
                            if (rs.wasNull()) {
                                dataOut.writeByte(0);
                                continue;
                            }
                            dataOut.writeByte(1);
                            dataOut.writeByte(i ? 1 : 0);
                            break;
                            
                        case 10: // big decimal
                            java.math.BigDecimal i = rs.getBigDecimal(ind);
                            if (rs.wasNull()) {
                                dataOut.writeByte(0);
                                continue;
                            }
                            dataOut.writeByte(1);
                            writeString(i.toString());
                            break;
                            
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
            writeError(e)
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
