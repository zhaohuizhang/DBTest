/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package mongotest;
import java.sql.*;
/**
 *
 * @author FarmerLuo
 */
public class mysqldb {
    public Connection mysql_conn;
    
    public mysqldb(){
        this.mysql_conn = null;
    }
    
    public mysqldb(String host,String username,String passwd,String dbname){
        this.mysql_conn = null;
        this.mysqldb_connect(host, username, passwd, dbname);
    }

    public Connection mysqldb_connect(String host,String username,String passwd,String dbname){

        try {
        	try {
				Class.forName( "org.gjt.mm.mysql.Driver" );
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
            this.mysql_conn = DriverManager.getConnection("jdbc:mysql://" + host + "/" + dbname + "?user=" + username + "&password=" + passwd + "&characterEncoding=UTF8");
        } catch (SQLException ex) {
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }

        return this.mysql_conn;
    }

    public void mysqldb_insert(long len) throws SQLException {

        Statement stmt = null;
        stmt = this.mysql_conn.createStatement();

        String str = "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456";

        for( int j = 0; j < len; j++ ){
            String sql = "insert into test (count, test1, test2, test3, test4) values (" + j + ",'" + str + "','" + str + "','" + str + "','" + str + "')";
            stmt.executeUpdate(sql);
        }
        stmt.close();
        this.mysql_conn.close();
    }


    public  void mysqldb_update(long len) throws SQLException {

        Statement stmt = null;
        stmt = this.mysql_conn.createStatement();

        String str = "UPDATE7890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456";

        //System.out.print(sql);
        for( int j = 0; j < len; j++ ){
            String sql = "update test set test1 = '" + str + "',test2 = '" + str + "' , test3 = '" + str + "', test4 = '" + str + "' where count = "+ j;
            stmt.executeUpdate(sql);
        }
        stmt.close();
        this.mysql_conn.close();
    }

    public  void mysqldb_select(long len) throws SQLException {

        Statement stmt = null;
        stmt = this.mysql_conn.createStatement();

        for( int j = 1; j <= len; j++ ){

            String sql = "select * from test where count = " + j;

            ResultSet sqlRst = stmt.executeQuery(sql);
            sqlRst.next();
            
//            int id = sqlRst.getInt("id");
            int count = sqlRst.getInt("count");
            String test1 = sqlRst.getString("test1");
            String test2 = sqlRst.getString("test2");
            String test3 = sqlRst.getString("test3");
            String test4 = sqlRst.getString("test4");
            //System.out.println(id + " " + count + " " + test1 + " " + test2 + " " + test3 + " " + test4 + "\n");
            sqlRst.close();
        }
        stmt.close();
        this.mysql_conn.close();

    }
}
