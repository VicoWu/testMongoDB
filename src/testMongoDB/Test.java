package testMongoDB;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class Test {
public static void main(String [] args){
	OutputStreamWriter osw;
	FileOutputStream out;
	 BufferedWriter bw=null ;
	 for(int i=0;i<5;i++){
	try {
		out = new FileOutputStream("/home/wuchang/delete.txt",true);
		  osw = new OutputStreamWriter(out);
		   bw = new BufferedWriter(osw);
		   
		    bw.write(String.valueOf(1));
		    bw.newLine();
		  
	} catch (FileNotFoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
	 }
	try {
		bw.close();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
}
}
