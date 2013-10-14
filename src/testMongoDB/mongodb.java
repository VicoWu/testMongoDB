/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package testMongoDB;
import com.mongodb.*;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * 
 * @author mrs
 *
 */


public class mongodb extends Thread {
	public static String dbname = "genbankDB";
	public String collectionName;
    public Mongo mongo_conn;
    public String operation;
    public long len;
    public int threadnum;
    public ArrayList<String> arrayList;
    public String host;
    public int port;
    public mongodb(String operation,long len,ArrayList<String> arrayList, String collectionName,Mongo mongo_conn){
    	this.operation=operation;
        this.len = len;
        this.arrayList=arrayList;
       // this.host=host;
       // this.port=port;
        this.mongo_conn=mongo_conn;
        this.collectionName=collectionName;
       // this.mongo_conn=mongodb_connect();
    }

    public Mongo mongodb_connect(){

        try {
            this.mongo_conn = new Mongo(this.host, this.port);
        } catch (UnknownHostException ex) {
            System.out.println("UnknownHostException:" + ex.getMessage());
        } catch (MongoException ex) {
            System.out.println("Mongo Exception:" + ex.getMessage());
            System.out.println("Mongo error code:" + ex.getCode());
        }

        return this.mongo_conn;
    }

    @Override
    public void run(){

        if ( this.operation.equals("insert") ) {
            this.mongodb_insert();
        } else if ( this.operation.equals("update") ) {
            this.mongodb_update();
        } else if( this.operation.equals("query")) {
            this.mongodb_select();
        } else{
        	this.mongodb_miss_select();
        }

    }

    public void mongodb_insert(){

        DB db = this.mongo_conn.getDB( dbname );

        DBCollection dbcoll = db.getCollection(this.collectionName);
      //  String str = "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456";
        String str="locus_test";
        String accession="accession_test";
        for (long j = 0; j < this.len; j++) {
            DBObject dblist = new BasicDBObject();
            dblist.put("indexNumber", j);
            dblist.put("LOCUS", str);
            dblist.put("accession", accession);
            //dblist.put("test3", str);
           //dblist.put("test4", str);
            try {
                dbcoll.insert(dblist);
            } catch (MongoException ex) {
                System.out.println("Mongo Exception:" + ex.getMessage());
                System.out.println("Mongo error code:" + ex.getCode());
            }
        }

    }

    public void mongodb_update(){

        DB db = this.mongo_conn.getDB( dbname );

        DBCollection dbcoll = db.getCollection(this.collectionName);
       // String str = "UPDATE7890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456";
        for (int j = 0; j < this.len; j++) {
            DBObject dblist = new BasicDBObject();
            DBObject qlist = new BasicDBObject();
            DBObject updatedValue= new BasicDBObject();
            updatedValue.put("indexNumber", 0);
            qlist.put("LOCUS", this.arrayList.get(j));
           // qlist.put("LOCUS", "locus_test");
            dblist.put("$set", updatedValue);
            try {
                dbcoll.update(qlist,dblist);
            } catch (MongoException ex) {
                System.out.println("Mongo Exception:" + ex.getMessage());
                System.out.println("Mongo error code:" + ex.getCode());
            }
        }
    }

    public void mongodb_select(){
        DB db = this.mongo_conn.getDB( dbname );

        DBCollection dbcoll = db.getCollection(this.collectionName);
       // System.out.println(this.len);
        for (int j = 0; j < this.len; j++) {
            BasicDBObject query = new BasicDBObject();
            //System.out.println(this.arrayList.get(j));
            query.put("LOCUS",this.arrayList.get(j));
            try {
                List objre =  dbcoll.find(query).toArray();
                //打印查询结果
               for ( Object x : objre ) {
                  //System.out.println(x);
               }
            } catch (MongoException ex) {
                System.out.println("Mongo Exception:" + ex.getMessage());
                System.out.println("Mongo error code:" + ex.getCode());
            }
        }
    }
    
    public void mongodb_miss_select(){
        DB db = this.mongo_conn.getDB( dbname );

        DBCollection dbcoll = db.getCollection(this.collectionName);
        
        for (int j = 0; j < this.len; j++) {
            BasicDBObject query = new BasicDBObject();
            query.put("LOCUS",String.valueOf(j));
            try {
                List objre =  dbcoll.find(query).toArray();
                //打印查询结果
               for ( Object x : objre ) {
                  //System.out.println(x);
               }
            } catch (MongoException ex) {
                System.out.println("Mongo Exception:" + ex.getMessage());
                System.out.println("Mongo error code:" + ex.getCode());
            }
        }
    }
}
