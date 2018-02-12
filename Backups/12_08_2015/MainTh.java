import java.io.*;
import java.util.*;
import java.sql.*;

public class MainTh extends Thread
{
 int tofetch;
 String response="HTTP/1.0 200 OK";
 Connection connection;
 String query = "";
 Statement st;
 PreparedStatement prep;
 
 String list_to_fetch="";
 
 WorkerTh[] concurrentThreads;
 

//////////////////short code 1 -MTN- destination (nradio service)/////////////////////////////
//String shortcode1="6631";
//String IP1="127.0.0.1";
//int Port1=80;
//String success1="OK";
/////////////////////////////////////////////// ////////////////////////////////////////
 

//////////////////short code 2 -SudaTel- destination (nradio service)/////////////////////////////
String shortcode2="+2491653";
String IP2="127.0.0.1";
int Port2=80;
String success2="OK";
/////////////////////////////////////////////// ////////////////////////////////////////

//////////////////short code 3 -MTN- destination (hawa alsudan radio service)/////////////////////////////
String shortcode3="1421";
String IP3="127.0.0.1";
int Port3=80;
String success3="OK";
/////////////////////////////////////////////// ////////////////////////////////////////

//////////////////short code 4 -SudaTel- destination (hawa alsudan radio service)/////////////////////////////
String shortcode4="+2491421";
String IP4="127.0.0.1";
int Port4=80;
String success4="OK";
/////////////////////////////////////////////// //////////////////////////////////////// 
 
 
/* 
//////////////////short code 3 destination /////////////////////////////
String shortcode3="6631";
String IP3="192.168.10.5";
int Port3=8080;
String success3="OK";
/////////////////////////////////////////////// ////////////////////////////////////////
*/






 public MainTh()
 {

    tofetch = 1;
    
    // list_to_fetch = "and shortcode in ('"+ shortcode1     "')";     
     list_to_fetch = "and shortcode in ('" + shortcode2 +  "','" + shortcode3 + "','" + shortcode4 +  "')"; 
    // list_to_fetch = "and shortcode in ('"+ shortcode1 + "','" + shortcode2 + "','" + shortcode3 + "')"; 
    
    query = "select id  from sms.mo_cdrs where  status='pending' " + list_to_fetch + " order by id limit 1";
      	
    

    try
    {
     DataBaseConnect();
    }catch(Exception e){System.out.println("Error occured While connecting to database. ");System.exit(0);}
    
    //Threads instantiation
    concurrentThreads = new WorkerTh[tofetch];

   
   	
 }
 
 public synchronized int FetchandLockRow() throws Exception
 {
 	int row=0;

    ResultSet rs;
    rs = null; 
         	  
 	rs = st.executeQuery(query);

 	  if(rs.next())
 	  {
 	  	row = rs.getInt(1);
  	  	rs.close(); 	  	
        prep.setInt(1, row); 
        prep.executeUpdate();
        prep.clearParameters();        
 
 	  }
 	    
  	
 	return row;
 }
 
 public void run()
 {
 //	System.out.println("run");
 	try
 	{
 	 for (int i=0;i<tofetch;i++)
   	 { 
   	  concurrentThreads[i] = new WorkerTh(this,i);
   	  concurrentThreads[i].start();
   	  Thread.sleep(500);
   	 }

 	 for (int i=0;i<tofetch;i++)
   	 { 
   	  concurrentThreads[i].join();
     }
    
 	}catch(Exception e){System.out.println("Error occured While instatiating Threads.");}
 }
 	



  	
 

  	
  		

     	
 	
 
  
  public void DataBaseConnect() throws Exception
  {
  	
       Class.forName("com.mysql.jdbc.Driver");
	   connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/sms","root", "");


        

    st = connection.createStatement();   
    
    prep = connection.prepareStatement("UPDATE  sms.mo_cdrs SET status='inprogress' WHERE (ID = ?)");
  }  

   
 public static void main(String arg[])
 {
 	MainTh oc = new MainTh();
 	oc.start(); 
 	try{oc.join();}catch(Exception e){}
 }
	
}