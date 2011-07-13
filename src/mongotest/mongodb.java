/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package mongotest;
import com.mongodb.*;
import java.net.UnknownHostException;
import java.util.List;

/**
 *
 * @author FarmerLuo
 */
public class mongodb {
    public Mongo mongo_conn;
    public String dbname;

    public mongodb(){
        this.mongo_conn = null;
        this.dbname = null;
    }

    public mongodb(String host,String dbname){
        this.mongo_conn = null;
        this.dbname = dbname;
        this.mongodb_connect(host,27017);
    }

    public mongodb(String host,String dbname,int port){
        this.mongo_conn = null;
        this.dbname = dbname;
        this.mongodb_connect(host,port);
    }

    public Mongo mongodb_connect(String host,int port){

        try {
            this.mongo_conn = new Mongo(host, port);
        } catch (UnknownHostException ex) {
            System.out.println("UnknownHostException:" + ex.getMessage());
        } catch (MongoException ex) {
            System.out.println("Mongo Exception:" + ex.getMessage());
            System.out.println("Mongo error code:" + ex.getCode());
        }

        return this.mongo_conn;
    }

    public void mongodb_insert(long len){


        DB db = this.mongo_conn.getDB( this.dbname );

        DBCollection dbcoll = db.getCollection("test");
        String str = "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456";
        for (long j = 0; j < len; j++) {
            DBObject dblist = new BasicDBObject();
            dblist.put("_id", j);
            dblist.put("count", j);
            dblist.put("test1", str);
            dblist.put("test2", str);
            dblist.put("test3", str);
            dblist.put("test4", str);
            try {
                dbcoll.insert(dblist);
            } catch (MongoException ex) {
                System.out.println("Mongo Exception:" + ex.getMessage());
                System.out.println("Mongo error code:" + ex.getCode());
            }
            //System.out.print(j + "\n");
        }

    }

    public void mongodb_update(long len){

        DB db = this.mongo_conn.getDB( this.dbname );

        DBCollection dbcoll = db.getCollection("test");
        String str = "UPDATE7890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456";
        for (long j = 0; j < len; j++) {
            DBObject dblist = new BasicDBObject();
            DBObject qlist = new BasicDBObject();
            qlist.put("_id", j);
            dblist.put("count", j);
            dblist.put("test1", str);
            dblist.put("test2", str);
            dblist.put("test3", str);
            dblist.put("test4", str);
            try {
                dbcoll.update(qlist,dblist);
            } catch (MongoException ex) {
                System.out.println("Mongo Exception:" + ex.getMessage());
                System.out.println("Mongo error code:" + ex.getCode());
            }
            //System.out.print(j + "\n");
        }
    }

    public void mongodb_select(long len){

        DB db = this.mongo_conn.getDB( this.dbname );

        DBCollection dbcoll = db.getCollection("test");

        for (long j = 0; j < len; j++) {
            BasicDBObject query = new BasicDBObject();
            query.put("_id", j);
            try {
                List objre =  dbcoll.find(query).toArray();

                //打印查询结果
//                for ( Object x : objre ) {
//                     System.out.println(x);
//                }
            } catch (MongoException ex) {
                System.out.println("Mongo Exception:" + ex.getMessage());
                System.out.println("Mongo error code:" + ex.getCode());
            }
        }
    }
}
