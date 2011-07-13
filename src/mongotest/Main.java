package mongotest;

import java.sql.SQLException;

/**
 *
 * @author FarmerLuo
 */
public class Main {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

        long rows = 0;
        long start = System.currentTimeMillis();

        if ( args.length < 4 ) {
            usage(1);
        }

        if ( args[0].equals("mysql") && args.length < 7 ) {
            usage(2);
        }

        rows = Long.parseLong(args[2]);

        if ( args[0].equals("mysql") ) {
            mysqldb mysqltest = new mysqldb(args[3],args[4],args[5],args[6]);
            if ( args[1].equals("insert") ) {
                try {
                    mysqltest.mysqldb_insert(rows);
                } catch (SQLException ex) {
                    System.out.println("SQLException: " + ex.getMessage());
                    System.out.println("SQLState: " + ex.getSQLState());
                    System.out.println("VendorError: " + ex.getErrorCode());
                }
            } else if ( args[1].equals("update") ) {
                try {
                    mysqltest.mysqldb_update(rows);
                } catch (SQLException ex) {
                    System.out.println("SQLException: " + ex.getMessage());
                    System.out.println("SQLState: " + ex.getSQLState());
                    System.out.println("VendorError: " + ex.getErrorCode());
                }
            } else if ( args[1].equals("select") ){
                try {
                    mysqltest.mysqldb_select(rows);
                } catch (SQLException ex) {
                    System.out.println("SQLException: " + ex.getMessage());
                    System.out.println("SQLState: " + ex.getSQLState());
                    System.out.println("VendorError: " + ex.getErrorCode());
                }
            } else {
                usage(4);
            }
        } else if ( args[0].equals("mongo") ) {
            mongodb mongotest = new mongodb(args[3],args[4]);
            if ( args[1].equals("insert") ) {
                mongotest.mongodb_insert(rows);
            } else if (args[1].equals("update")) {
                mongotest.mongodb_update(rows);
            } else if (args[1].equals("select")) {
                mongotest.mongodb_select(rows);
            }  else {
                usage(5);
            }
        } else {
            usage(3);
        }

        long stop = System.currentTimeMillis();
        long endtime = (stop - start)/1000;
        if ( endtime == 0 ) endtime = 1;
        long result = rows/endtime;
        
        System.out.print("Total run time:" + endtime + " sec\n");
        System.out.print("Total rows:" + rows + "\n");
        System.out.print(args[0] + " " + args[1] + " Result:" + result + "row/sec\n");
    }

    public static void usage(int errorno){
        System.out.print("Usage:\n");
        System.out.print("mysql test:\n");
        System.out.print("java -jar mongotest.jar < mysql > < [select | update | insert] > < rows >  < host > < username > < password>  <database> \n");
        System.out.print("mongo test:\n");
        System.out.print("java -jar mongotest.jar < mongo > < [select | update | insert] > < rows >  < host > <database> \n");
        System.exit( errorno );
    }

}
