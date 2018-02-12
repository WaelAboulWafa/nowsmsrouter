import java.io.*;
import java.util.*;
import java.sql.*;
import java.text.*;
import java.io.*;
import java.net.*;

public class WorkerTh extends Thread
{

 MainTh Opconnector;
 
 int myrow=0;


 
 
 int i;
 
 

String shortcode="";

String provider="";
String msisdn="";
String contents="";
String lang="";


 PreparedStatement prep1; 
 
 String query1="SELECT shortcode,msisdn,Contents,Lang,provider   from sms.mo_cdrs where (id=?)";
 
 //successful request
 PreparedStatement prep2;
 String query2="UPDATE    sms.mo_cdrs SET status='sent', dateout= now()  WHERE (id = ?)";

 //failed request
 PreparedStatement prep4;
 String query4="UPDATE    sms.mo_cdrs SET status='failed'  WHERE (id = ?)";
 
 
 
  
 boolean AmIAllowedtofetchANewRequest=true;
  
  
 public OutputStream logfile; 
 
 public WorkerTh(MainTh mainclass,int index)
 {
   	Opconnector = mainclass;i=index;
   	
   	
   	try{

     	 logfile = new FileOutputStream("Logs/"+"Trace_"+ ((new SimpleDateFormat ("yyyy-MM-dd")).format(new java.util.Date())).trim() + "_" +Integer.toString(i) +".txt",true);
   		
     	  logfile.write( ("<"+ ((new SimpleDateFormat ("yyyy-MM-dd kk:mm:ss.S")).format(new java.util.Date())).trim() +"> ").getBytes("ASCII") ); 
          logfile.write(("Thread Started.\r\n").getBytes("ASCII"));    	
   		
   	     prep1 = Opconnector.connection.prepareStatement(query1);
   	     prep2 = Opconnector.connection.prepareStatement(query2);
  	        	     
   	     prep4 = Opconnector.connection.prepareStatement(query4);   	        	     
   	     
       }catch(Exception e){
       	                   System.out.println("error while preparing the statements.");
       	                   System.out.println(e);
       	                   try{
       	                   	   logfile.write( ("<"+ ((new SimpleDateFormat ("yyyy-MM-dd kk:mm:ss.S")).format(new java.util.Date())).trim() +"> ").getBytes("ASCII") ); 
                               logfile.write (( e.toString() +"\r\n").getBytes("ASCII"));    	
                              }catch(Exception e1){}
       	                  }
 }
 
 public void run()
 {
 	
 	try
 	{
     while(true)
     {
         logfile.close();logfile=null;
     	 logfile = new FileOutputStream("Logs/"+"Trace_"+ ((new SimpleDateFormat ("yyyy-MM-dd")).format(new java.util.Date())).trim() + "_" +Integer.toString(i) +".txt",true);
     	if(AmIAllowedtofetchANewRequest==true)
     	{
     	  logfile.write( ("<"+ ((new SimpleDateFormat ("yyyy-MM-dd kk:mm:ss.S")).format(new java.util.Date())).trim() +"> ").getBytes("ASCII") ); 
          logfile.write(("Trying to fetch a new request ...\r\n").getBytes("ASCII"));    	
          
	       myrow=Opconnector.FetchandLockRow();
	       
         if(myrow != 0)
         { 
       	  logfile.write( ("<"+ ((new SimpleDateFormat ("yyyy-MM-dd kk:mm:ss.S")).format(new java.util.Date())).trim() +"> ").getBytes("ASCII") ); 
          logfile.write(("Request N# " + Integer.toString(myrow) + " fetched successfuly.\r\n").getBytes("ASCII"));    	
 
     	  logfile.write( ("<"+ ((new SimpleDateFormat ("yyyy-MM-dd kk:mm:ss.S")).format(new java.util.Date())).trim() +"> ").getBytes("ASCII") ); 
          logfile.write(("Trying to load request N# " + Integer.toString(myrow) + " ...\r\n").getBytes("ASCII"));    	
        	
       	  LoadRequest();

       	  logfile.write( ("<"+ ((new SimpleDateFormat ("yyyy-MM-dd kk:mm:ss.S")).format(new java.util.Date())).trim() +"> ").getBytes("ASCII") ); 
          logfile.write(("Request N# " + Integer.toString(myrow) + " loaded successfuly.\r\n").getBytes("ASCII"));    	


 	           	  
        	 
      	  String thehttprequest="";
          thehttprequest=Generate_HTTP_Request();
       
//        System.out.println("The URL = "+thehttprequest); 

// 	      Opconnector.connection.close();
// 	      System.exit(0);  

        	   
          AmIAllowedtofetchANewRequest = false;        	   
              
          logfile.write( ("<"+ ((new SimpleDateFormat ("yyyy-MM-dd kk:mm:ss.S")).format(new java.util.Date())).trim() +"> ").getBytes("ASCII") ); 
          logfile.write(("Trying to send request N# " + Integer.toString(myrow) + " ...\r\n").getBytes("ASCII"));    	

          SendandLogRequest(thehttprequest);    
        	  

        	    	        	        	        	
          Thread.sleep(1000);        	 

         }
         else
         {
           //nothing in queue..	
           //System.out.println("nothing in queue ... going to sleep 5 seconds");	
        	  
       	   logfile.write( ("<"+ ((new SimpleDateFormat ("yyyy-MM-dd kk:mm:ss.S")).format(new java.util.Date())).trim() +"> ").getBytes("ASCII") ); 
           logfile.write(("Empty queue.\r\n").getBytes("ASCII"));    	
           
           Thread.sleep(15000);
         }
       }
       else
       {
       	
         logfile.write( ("<"+ ((new SimpleDateFormat ("yyyy-MM-dd kk:mm:ss.S")).format(new java.util.Date())).trim() +"> ").getBytes("ASCII") ); 
         logfile.write(("RETrying to send request N# " + Integer.toString(myrow) + " ...\r\n").getBytes("ASCII"));    	
              
         //try to REsend the same request another time
         System.out.println("Retrying request n# : " + Integer.toString(myrow));
         String thehttprequest="";
         thehttprequest=Generate_HTTP_Request();  
        	   

         SendandLogRequest(thehttprequest);   
        	   
//   	     logfile.write( ("<"+ ((new SimpleDateFormat ("yyyy-MM-dd hh:mm:ss.S")).format(new java.util.Date())).trim() +"> ").getBytes("ASCII") ); 
 //        logfile.write(("Request N# " + Integer.toString(myrow) + " REsent successfuly after retry.\r\n").getBytes("ASCII"));    	
 
        	      	        	        	        	
         Thread.sleep(1000);        	 

        
       }
     }
    
 	}catch(Exception e){
 		                 System.out.println(e);
       	                   try{
       	                   	   logfile.write( ("<"+ ((new SimpleDateFormat ("yyyy-MM-dd kk:mm:ss.S")).format(new java.util.Date())).trim() +"> ").getBytes("ASCII") ); 
                               logfile.write (( e.toString() +"\r\n").getBytes("ASCII"));    	
                              }catch(Exception e1){}
 		                 
 		               }
 }
 	

 
 public void LoadRequest() throws Exception
 {
 	
       ResultSet rs=null;
       prep1.setInt(1,myrow); 
       rs= prep1.executeQuery();
       if(rs.next())
 	   {
 	   	
        shortcode =    (rs.getString(1)).trim(); 	   	
 	 	msisdn =    (rs.getString(2)).trim();
 	 	contents =    (rs.getString(3)).trim();
        lang =    (rs.getString(4)).trim();
        provider=    (rs.getString(5)).trim();
 	 	
      
        rs.close();
        prep1.clearParameters();

       
 	   }
 	   

 	   

 }
	
	
    	

    	public void SendandLogRequest(String thereq) throws Exception
    	{
    	 
    	  String remain="";
    	  PrintWriter out; 
          BufferedReader in;
          Socket s;
          ByteArrayOutputStream fout; 
          
          String currentIP="";
          int currentPort=0;
          String currentsuccess="";
    	 try
    	 { 
	  
    	   currentIP="";currentPort=0;currentsuccess="";
    	   //if(shortcode.equalsIgnoreCase(Opconnector.shortcode1) ){currentIP=Opconnector.IP1; currentPort=Opconnector.Port1;currentsuccess=Opconnector.success1;}
    	   if(shortcode.equalsIgnoreCase(Opconnector.shortcode2) ){currentIP=Opconnector.IP2; currentPort=Opconnector.Port2;currentsuccess=Opconnector.success2;}    	   
    	   if(shortcode.equalsIgnoreCase(Opconnector.shortcode3) ){currentIP=Opconnector.IP3; currentPort=Opconnector.Port3;currentsuccess=Opconnector.success3;}
    	   if(shortcode.equalsIgnoreCase(Opconnector.shortcode4) ){currentIP=Opconnector.IP4; currentPort=Opconnector.Port4;currentsuccess=Opconnector.success4;}
    	//   if(shortcode.equalsIgnoreCase(Opconnector.shortcode5) ){currentIP=Opconnector.IP5; currentPort=Opconnector.Port5;currentsuccess=Opconnector.success5;}    	   
    	       	       	   
	       	       	   
    	   
    	   
    	   if(currentPort != 0)
    	   { 
       	   logfile.write( ("<"+ ((new SimpleDateFormat ("yyyy-MM-dd kk:mm:ss.S")).format(new java.util.Date())).trim() +"> ").getBytes("ASCII") ); 
           logfile.write (("Trying : " +currentIP +":" + Integer.toString(currentPort)+"\r\n").getBytes("ASCII"));

    	     s = new Socket(currentIP,currentPort);
    	   }
    	   else
    	   {
             logfile.write( ("<"+ ((new SimpleDateFormat ("yyyy-MM-dd kk:mm:ss.S")).format(new java.util.Date())).trim() +"> ").getBytes("ASCII") ); 
             logfile.write (("request N# : "+ Integer.toString(myrow)+ " , No routing rule found." +"\r\n").getBytes("ASCII"));   
             AmIAllowedtofetchANewRequest=true;             
             return;     	   	
    	   }
    	   
    	   
    	   out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(s.getOutputStream())),false);        
	  	   in = new BufferedReader(new InputStreamReader(s.getInputStream())); 	
  	  	   out.println(thereq);
	  	   out.println("\r\n");
	  	   out.flush();
	  	   System.out.println(thereq);	  

      	   logfile.write( ("<"+ ((new SimpleDateFormat ("yyyy-MM-dd kk:mm:ss.S")).format(new java.util.Date())).trim() +"> ").getBytes("ASCII") ); 
           logfile.write (( thereq +"\r\n").getBytes("ASCII"));

	       //read response
           fout = new ByteArrayOutputStream();
           int b;
           while((b = in.read()) != -1) fout.write(b);
           remain = fout.toString();

           in.close();out.close();s.close();
           in= null;out=null;s=null;fout=null;
           System.out.println(remain);

      	   logfile.write( ("<"+ ((new SimpleDateFormat ("yyyy-MM-dd kk:mm:ss.S")).format(new java.util.Date())).trim() +"> ").getBytes("ASCII") ); 
           logfile.write (( remain +"\r\n").getBytes("ASCII"));    	

           
          }catch(Exception e){
          	       	             try{
       	                           	  logfile.write( ("<"+ ((new SimpleDateFormat ("yyyy-MM-dd kk:mm:ss.S")).format(new java.util.Date())).trim() +"> ").getBytes("ASCII") ); 
                                      logfile.write (( e.toString() +"\r\n").getBytes("ASCII"));  
                                      LogFAILEDRequest();  
                                       AmIAllowedtofetchANewRequest=true;	
                                    }catch(Exception e1){}

          	                   System.out.println("Error occured while establishing/sending/receiving connection/request/response" + e); 
          	                   in= null;out=null;s=null;fout=null;
          	                   return;
                             }
          
           //log the request IF & ONLY IF a successful feedback is returned from gateway.
          if(remain.endsWith(currentsuccess))
           {
           	prep2.setInt(1,myrow);
        	prep2.executeUpdate();
      	    prep2.clearParameters();      
      	    
            logfile.write( ("<"+ ((new SimpleDateFormat ("yyyy-MM-dd kk:mm:ss.S")).format(new java.util.Date())).trim() +"> ").getBytes("ASCII") ); 
            logfile.write(("Request N# " + Integer.toString(myrow) + " sent successfuly.\r\n").getBytes("ASCII"));    	      	    
      	    
           }
           else
           { 
             logfile.write( ("<"+ ((new SimpleDateFormat ("yyyy-MM-dd kk:mm:ss.S")).format(new java.util.Date())).trim() +"> ").getBytes("ASCII") ); 
             logfile.write (("UNsuccessful response received from gateway for request N# : "+ Integer.toString(myrow)+ "\r\n").getBytes("ASCII"));   
      //       logfile.write( ("<"+ ((new SimpleDateFormat ("yyyy-MM-dd kk:mm:ss.S")).format(new java.util.Date())).trim() +"> ").getBytes("ASCII") );              
      //       logfile.write ((remain + "\r\n").getBytes("ASCII"));   
                                                       
   //          System.out.println("UNsuccessful response received from gateway for request n# : "+ Integer.toString(myrow));
   //          System.out.println(remain);
             
             System.out.println("marking this request as FAILED and fetch another one");
             
             logfile.write( ("<"+ ((new SimpleDateFormat ("yyyy-MM-dd kk:mm:ss.S")).format(new java.util.Date())).trim() +"> ").getBytes("ASCII") ); 
             logfile.write (("Marking request N# : "+ Integer.toString(myrow)+ " as FAILED and fetch another one." +"\r\n").getBytes("ASCII"));   
             
             
             LogFAILEDRequest();

            }
            
            AmIAllowedtofetchANewRequest=true;
            
            	  
    	}
    	
    	public void LogFAILEDRequest() throws Exception
    	{
   		  prep4.setInt(1,myrow);
          prep4.executeUpdate();
      	  prep4.clearParameters(); 
    	}
    
    
       public String Generate_HTTP_Request()
       {

        String therequest ="";


        
         //shortcode 1,2 (nradio service)
    	   if( shortcode.equalsIgnoreCase(Opconnector.shortcode2)   )
    	   {
    	   	
            therequest = "GET /nradio_pgm/submit.jsp?" +
                    "provider="+ URLEncoder.encode(provider.trim())  + 
                    "&shortcode="+ URLEncoder.encode(shortcode.trim())  + 
                    "&msisdn="+ URLEncoder.encode(msisdn.trim())  + 
                    "&contents="+ URLEncoder.encode(contents.trim())  + 
                    "&lang="+lang.trim() ; 

           }
 
 		// HawaAlsudan
 		   if( shortcode.equalsIgnoreCase(Opconnector.shortcode3)  ||   shortcode.equalsIgnoreCase(Opconnector.shortcode4)   )
    	   {
    	   	
            therequest = "GET /hawasudan_pgm/submit.jsp?" +
                    "provider="+ URLEncoder.encode(provider.trim())  + 
                    "&shortcode="+ URLEncoder.encode(shortcode.trim())  + 
                    "&msisdn="+ URLEncoder.encode(msisdn.trim())  + 
                    "&contents="+ URLEncoder.encode(contents.trim())  + 
                    "&lang="+lang.trim() ; 

           }
		   
		   
		   /*


			//SudaTel- destination(sahoortv)
    	   if( shortcode.equalsIgnoreCase(Opconnector.shortcode5)   )
    	   {
    	   	
            therequest = "GET /sahoortv/submit.jsp?" +
		    "provider=FIT&country=Sudan&operator=Sudatel" +
                    "&shortcode="+ URLEncoder.encode(shortcode.trim())  + 
                    "&msisdn="+ URLEncoder.encode(msisdn.trim())  + 
                    "&contents="+ URLEncoder.encode(contents.trim())  + 
                    "&lang="+lang.trim() ; 

           }
		   */
                          
           
            
            return therequest;
       	
       }
       
       
 




      
       
       public String unicodeMsg(String readable)	
       {
       	String x=readable;//space
       	
       	try
       	{
       		

       		    byte hexstring[];
     int  data[];

    hexstring= x.getBytes("UTF-16");
   	    
   	    data = new int[hexstring.length - 2];
                    for(int i = 2; i < hexstring.length; i++)
                    {
                        int k = (new Byte(hexstring[i])).intValue();
                        data[i - 2] = k;
                    }
        
  
    
     
      StringBuffer strb = new StringBuffer();
     
      for (int i=0;i<data.length;i++)
      {
       if( data[i] < 16)
       {
       	strb.append("0");
       	strb.append(Integer.toHexString(data[i]));
       }
       else
       {
       	strb.append(Integer.toHexString(data[i]));
       }
        	
      }      
       
      String theHexString = strb.toString();        
      theHexString= theHexString.toUpperCase();
      
       		x=theHexString;
       		
       		
       	}catch(Exception e){x="";}
       	
       	
        	
       	
       	
       	if (x.length() ==0){x="0020";}
       	
//      	System.out.println(x);
       	return x;
       }





 
}

