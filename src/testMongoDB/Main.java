package testMongoDB;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.mongodb.Mongo;
import com.mongodb.MongoOptions;
import com.mongodb.ServerAddress;

/**
 *
 * @author FarmerLuo
 * @version 0.3
 * 
 * Add:
 * 1) mysql并发测试
 * 2）mongodb并发测试
 *
 * Last Date: 2010.04.15
 *
 */
public class Main {
     
   //  public static int threadNumber = 5;//1,10,100,1000
    // public static long docNumPerThread= 1000;;
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
    	int threadNumber= Integer.parseInt(args[0]);
    	int docNumPerThread=  Integer.parseInt(args[1]);
    	String op = args[2];
    	String host = args[3];
    	int port =Integer.parseInt(args[4]);
    	String collectionName = args[5];
    	System.out.println("threadNumber:"+String.valueOf(threadNumber)+" ,docNumPerThread:"+
    	String.valueOf(docNumPerThread)+" ,op:"+op+" ,Host:"+host+" ,port:"+port);
    	FileInputStream f = null;
    	ArrayList<String> locusList = new ArrayList<String>();
		try {
			f = new FileInputStream("/home/wuchang/length/locus.txt");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	BufferedReader reader = new BufferedReader(new InputStreamReader(f));
        for(int i =0;i<docNumPerThread;i++){
        	String line;
			try {
				line = reader.readLine();
				if(line == null){
					System.out.println("the total locus is less than the docNumPerThread");
				}
				else{
					locusList.add(line.trim());
				}
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
       
        }
        try {
			reader.close();
			f.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        MongoOptions options=new MongoOptions();
        options.autoConnectRetry=true;
        options.connectionsPerHost=50;
        options.maxWaitTime=5000;
        options.socketTimeout=0;
        options.connectTimeout=15000;
        options.threadsAllowedToBlockForConnectionMultiplier=5000;
        
        Mongo mongo_conn = null;
        try {
			mongo_conn = new Mongo(new ServerAddress(host, port),options);
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
    
            mongodb[] mongothread = new mongodb[threadNumber];
            for ( int k = 0; k < threadNumber; k++ ) {
                mongothread[k] = new mongodb(op,docNumPerThread,locusList,collectionName,mongo_conn);//query ,insert ,update
            }
            
            long start = System.currentTimeMillis();
            for(int k=0;k<threadNumber;k++)
            mongothread[k].start();
            
            for ( int k = 0; k < threadNumber; k++ ) {
//                System.out.println("mongothread["+k+"].isAlive()=" + mongothread[k].isAlive());
                if ( mongothread[k].isAlive() ) {
                    try {
                        mongothread[k].join();
                    } catch (InterruptedException ex) {
                        System.out.println(ex.getMessage());
                        System.out.println(ex.toString());
                    }
                }
            }

        long stop = System.currentTimeMillis();
        long endtime = (stop - start);
        if ( endtime == 0 ) endtime = 1;
       // long result = docNumPerThread/endtime;
        double tresult =(double) docNumPerThread*threadNumber/endtime;
        try {
			FileOutputStream out = new FileOutputStream("/home/wuchang/testMongoDB.txt",true);
		    OutputStreamWriter osw = new OutputStreamWriter(out);
		    BufferedWriter bw = new BufferedWriter(osw);
		    bw.newLine();
		    bw.write(String.valueOf(threadNumber)+"  "+
		    String.valueOf(docNumPerThread)+"  "+String.valueOf(endtime)
		    +"  "+String.valueOf(docNumPerThread*threadNumber)+"  "+String.valueOf(tresult)
		    		);
		    
		    bw.close();
        } catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
//        System.out.print("Total thread:" + threadNumber + "\n");
//        System.out.print("Total run time:" + endtime + " milliseconds\n");
//        System.out.print("Per-thread rows:" + docNumPerThread + "\n");
//        //System.out.print("Per-thread " + args[0] + " " + args[1] + " Result:" + result + "row/sec\n");
//        System.out.print("Total rows:" + docNumPerThread * threadNumber + "\n");
//        System.out.print("Total Result:" + tresult + " row/milliseconds\n");
    }



}
