/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package mongothread;
import java.sql.*;

/**
 *
 * @author FarmerLuo
 *
 * Mysql 测试�? *
 * 参数�? * @param public int threadnum:线程�? * @param public int len:每线程测试的记录�? * @param public String operation:测试模式 select insert update
 * @param public String dbname:mysql数据库名
 * @param public String host:mysql主机
 * @param public String username:mysql用户�? * @param public String password:mysql密码
 *
 */
public class mysqldb extends Thread  {
    public Connection mysql_conn;
    public String operation;
    public long len;
    public int threadnum;

    public mysqldb(String operation,long len,String host,String username,String passwd,String dbname,int threadnum){
        this.mysql_conn = null;
        this.operation = operation;
        this.len = len;
        this.threadnum = threadnum;
        this.mysqldb_connect(host, username, passwd, dbname);
    }

    public Connection mysqldb_connect(String host,String username,String passwd,String dbname){

        try {
            this.mysql_conn = DriverManager.getConnection("jdbc:mysql://" + host + "/" + dbname + "?user=" + username + "&password=" + passwd + "&characterEncoding=UTF8");
        } catch (SQLException ex) {
                ex.printStackTrace();
        }

        return this.mysql_conn;
    }

    @Override
    public void run(){

        if ( this.operation.equals("insert") ) {
            try {
                this.mysqldb_create();
                this.mysqldb_insert();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        } else if ( this.operation.equals("update") ) {
            try {
                this.mysqldb_update();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        } else {
            try {
                this.mysqldb_select();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }

    }

    //创建测试表，如果表已存在，将删除重建
    public void mysqldb_create() throws SQLException {

        Statement stmt = null;
        stmt = this.mysql_conn.createStatement();

        String sql = "DROP TABLE IF EXISTS `test" + this.threadnum + "`;";
        stmt.executeUpdate(sql);

        sql =  "CREATE TABLE test" + this.threadnum + " (  id int(11) NOT NULL auto_increment,  count int(11) NOT NULL default 0,  test1 varchar(256) NOT NULL,  test2 varchar(256) NOT NULL,  test3 varchar(256) NOT NULL,  test4 varchar(256) NOT NULL,  PRIMARY KEY  (id)) ENGINE=InnoDB  DEFAULT CHARSET=utf8 ";
        stmt.executeUpdate(sql);

        stmt.close();
        //this.mysql_conn.close();
    }


    public void mysqldb_insert() throws SQLException {

        Statement stmt = null;
        stmt = this.mysql_conn.createStatement();

        String str = "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456";

        for( int j = 0; j < this.len; j++ ){
            String sql = "insert into test" + this.threadnum + " (count, test1, test2, test3, test4) values (" + j + ",'" + str + "','" + str + "','" + str + "','" + str + "')";
            stmt.executeUpdate(sql);
        }
        stmt.close();
        this.mysql_conn.close();
    }


    public  void mysqldb_update() throws SQLException {

        Statement stmt = null;
        stmt = this.mysql_conn.createStatement();

        String str = "UPDATE7890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456";

        //System.out.print(sql);
        for( int j = 0; j < this.len; j++ ){
            String sql = "update test" + this.threadnum + " set test1 = '" + str + "',test2 = '" + str + "' , test3 = '" + str + "', test4 = '" + str + "' where id = "+ j;
            stmt.executeUpdate(sql);
        }
        stmt.close();
        this.mysql_conn.close();
    }

    public  void mysqldb_select() throws SQLException {

        Statement stmt = null;
        stmt = this.mysql_conn.createStatement();

        for( int j = 1; j <= len; j++ ){

            String sql = "select * from test" + this.threadnum + " where id = " + j;

            ResultSet sqlRst = stmt.executeQuery(sql);
            sqlRst.next();
            
            int id = sqlRst.getInt("id");
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
