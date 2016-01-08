/***********Original Author: Pratima Kshetry*************************************/

package PK;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Vector;

import javax.swing.JOptionPane;

import java.io.BufferedReader;
import java.io.BufferedWriter;


public class ClusterApp {
	
    private BufferedReader rdr=null;
    private BufferedWriter wrt=null;
    private BufferedWriter logWrt=null;
    private HashMap<String, Vector<String>> cluster=new HashMap<String,Vector<String>>();
    private long maxLineToProcess=6000;
    private long lineRead=0;
    private long  lSplit=0;
    private  long lineReadTotal=0;
    private String customer="";
    
	public void generateOutputSplits()
	{
		try
		{
			String outputFile="D:\\cluster\\output\\out"+lSplit+".txt";
			FileWriter file=new FileWriter(outputFile);	
			wrt=new BufferedWriter(file);
		    String outStr="";
		    //long lRecordCount=0;	
		    //long lRecordLastIndex=0;
		    for(String k:cluster.keySet())
		    {
		    	Vector<String> vals=cluster.get(k);
		    	outStr="";
		    	for(String s:vals)
		    	{
		    		outStr+=s+",";
		    	}
		    	outStr=outStr.substring(0,outStr.length()-1)+"\r\n";//remove last character
		    	wrt.write(k+":"+outStr);
		    	/*
		        if((lRecordCount-lRecordLastIndex)>1000)
				 {
					//out after mapping 1000 items.
					
					lRecordLastIndex=lRecordCount;
				 }*/
		        //lRecordCount++;
		    }

		    wrt.close();
		    System.out.println("Commited split: "+lSplit);
		    lSplit++;
		   
		    
			
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		finally
		{
			cluster.clear();
			System.gc();
		}
		
	}
	
	
	  public void StartClusteringTest(String inFile,String outFile) throws IOException
		{  
			try
			{			
				rdr=new BufferedReader (new FileReader(inFile));
				
				String line="";	  
			    while((line=rdr.readLine())!=null)
			    {			    	
			    	lineReadTotal++;	 	
	    	
				}
			    System.out.println("Total lines of Record read: "+lineReadTotal++);
			    rdr.close();
			    logWrt.close();
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}
	  
	  
	  
	  
    public void StartClustering(String inFile,String outFile) throws IOException
	{  
		try
		{	
			//rdr=new BufferedReader (new FileReader(inFile),1024*1024*10);	
		   	
		    long lRecordLastIndex=0;
		    
			rdr=new BufferedReader (new FileReader(inFile));
			logWrt=new BufferedWriter (new FileWriter("D:\\cluster\\log.txt"));	  	
		    String line="";
		    long lineReadTotal=0;
		    long lineReadTotalLast=0;
		   
		    while((line=rdr.readLine())!=null)
		    {
		    	cluster(line);
		    	lineReadTotal++;
		    	lineRead++;
		    	
			    	
			  if(lineRead-lRecordLastIndex>10000)System.out.println("Record read:"+lineRead);
		    	
			  
			  if(lineRead>maxLineToProcess)
		    	{
		    		System.out.println("Total lines of Record read: "+lineReadTotal++);
		    		generateOutputSplits();		    		
		    		lineRead=0;
		    		//System.out.println("Record read"+lineRead);
		    		
		    	}
			  
			  if((lineReadTotal-lineReadTotalLast) >5000000)
			  {
				 JOptionPane.showMessageDialog(null, "Processed lot of data");
				 lineReadTotalLast=lineReadTotal;
			  }
		    	
		    }		   
		    generateOutputSplits();
		    System.out.println("Finished...");
		    //Out Key,Map
		    //System.out.println("Record read final "+lineRead);
		    
		    
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			if (rdr!=null)rdr.close();
			if (wrt!=null)wrt.close();
			if (logWrt!=null)logWrt.close();
		}		

	}
    public void log(String msg)
    {
		try
		{
    	
			if(logWrt!=null)
			{
				logWrt.write("log:"+msg+"\r\n");
				logWrt.flush();
			}
		}
		catch(Exception e)
		{
			
		}
    	
    }
	
    public void cluster(String strValue) 
    {
		int ndx=strValue.indexOf(';');
		//remove customerID from the string first
		//Remaining will be productID;Rating-productID;Rating
		
		
		customer=strValue.substring(0,ndx);		
		String str1=strValue.substring(ndx+1,strValue.length());
		HashMap<String, String> oMap=new HashMap<String,String>();	
					
		String[] keyvalInfo=str1.split("-");
		int keyValCount=0;
		if(keyvalInfo.length>=2)//consider only if the customer has rated at least 5 products.
		{
			//Do not consider customer that have rated more than 100 products this can be scammers
			if(keyvalInfo.length>10000)
			{
				//alert scammer
				//String msg="Scammer alert: customer ID:"+ customer+"\nProducts Rated:"+keyvalInfo.length;
				String msg=customer+","+keyvalInfo.length+",Scammer";
				System.out.println(msg);
				log(msg);				
			}
			else
			{
				if(keyvalInfo.length>100)
				{
					 //log customers with more than 100 products rated
					//String msg="customer ID:"+ customer+"\nProducts Rated:"+keyvalInfo.length;		
					String msg=customer+","+keyvalInfo.length+",NonScammer";
					log(msg);
				}
				for(String strKeyValInfo:keyvalInfo)
				{
					String[] strKeyVal=strKeyValInfo.split(";");
					if(strKeyVal.length==2)
					{   //Filter redundant review by same customer
						if(!oMap.containsKey((strKeyVal[0])))
					    {
							oMap.put(strKeyVal[0], strKeyVal[1]);
							keyValCount++;
						}											
					}
				}
				
				String[][]kvArray=new String[keyValCount][2];
				int currentKeyVal=0;
				for(String k1:oMap.keySet())
				{
					if(currentKeyVal<keyValCount)
					{
						kvArray[currentKeyVal][0]=k1;
						kvArray[currentKeyVal][1]=oMap.get(k1);
						currentKeyVal++;
					}				  
				}			
				String k="",v="";	
				int clusterPoint=0;
				for(int i=0;i<keyValCount;i++)
				{
					for(int j=i+1;j<keyValCount;j++)
					{
					     if(i==j) continue;
						 k=kvArray[i][0]+"-"+kvArray[j][0];
					     v=kvArray[i][1]+"-"+kvArray[j][1];
					     if(!cluster.containsKey(k))
					     {
					    	 Vector<String> val =new Vector<String>();
					    	 val.add(v);
					    	 cluster.put(k, val);	
					    	 clusterPoint++;
					    	 if(clusterPoint>1000000)
					    	 {
					    		 System.out.println("Max Cluster Points reached");
					    		 generateOutputSplits();
					    		 clusterPoint=0;
					    	 }
					    	 
					     }
					     else
					     {
					    	 Vector<String> val =cluster.get(k);
					    	 val.add(v);
					     }					
					}
				}			
			}
			oMap.clear();
			oMap=null;
		}
    
    }



	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try
		{
	       ClusterApp app=new ClusterApp();
	       String inputFile,outputFile;
	       inputFile="D:\\cluster\\input\\stage.txt";
	       outputFile="D:\\cluster\\output\\stage1.txt";
	       app.StartClustering(inputFile,outputFile);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}

	}

}
