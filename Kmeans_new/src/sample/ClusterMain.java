package sample;

/*
 * 
 * Sources : This code is actuall based on logic of simple K means
1. The program can run, but there is no iteration [5 points]  Working fine
2. Correctly implement the required iteration [7 points]      Working fine
3. Correctly implement the counters [3 points]				  Working fine
{Please read file name "centroid"  to get information about centroid and number of iterations.This file should be in the homedir 
where we run the test cases}
4. Output results correct [5 points]                          Working fine

 *
 *#<!--
SAMPLE DATA TYPE: User with no of followers like below
---------------------------------------------------------
users with number of followers:-
Please go through README.md to get more details on sample tweeter code used to 
download such data-->
#######################
InBoldRebirth 155     |
Rae_laVidaBella 439   |
2turntgilinsky 681    |
RobertDFlesher 5      |
ACMsPrincess 117      |
bigbooty_judy8 470    |
AmyMcHaney 1096       |
son_ofa_GUNN 411      |
kaykassssss 100       |
Your_Heinous 647      |
#######################
-->
 * 
 * TWITTER CODE USED
 * ---------------------
 * public void onStatus(Status status) {
		String tweetText = status.getText();
	    	
		if(isEnglish(tweetText)) {
			LOG.info(status.getText());
			twitter4j.User  user = status.getUser();
		System.out.println(status.getUser().getScreenName()+"\t"+  status.getUser().getId());
		long userid = status.getUser().getId();
		
		
		}
	}

 * */
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterMain {

	private static final transient Logger LOG = LoggerFactory.getLogger(ClusterMain.class);
	public static String OUTPATH ="/output";
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
			System.out.println("Program started");
			
			Configuration conf = new Configuration();		
            
			LOG.info("HDFS Root Path: {}", conf.get("fs.defaultFS"));
			LOG.info("MR Framework: {}", conf.get("mapreduce.framework.name"));
			/* Set the Input/Output Paths on HDFS */
			String inputPath = "/input",inputFrom=inputPath;
			String outputPath = OUTPATH ;
			String centroid = "/centroid";
			createFile(conf, centroid);
			
			
			int count=0;
			Boolean TRUE = true;
   	 while (TRUE)
			{
			
			deleteFolder(conf,outputPath);
			Job job;
			job = Job.getInstance(conf);
			System.out.println("Location 1");
	        job.setOutputKeyClass(IntWritable.class);
	        job.setOutputValueClass(Text.class);
	        job.setJarByClass(ClusterMain.class);
	        job.setMapperClass(ClusterMapper.class);
	        job.setReducerClass(ClusterReducer.class);
	        job.setInputFormatClass(TextInputFormat.class);//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
	        job.setOutputFormatClass(TextOutputFormat.class);
			System.out.println("Location 2");
			FileInputFormat.addInputPath(job, new Path(inputFrom));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			System.out.println("Location 3");
			
				job.waitForCompletion(true);
				
				//adding newLine to centroid file
				FileSystem fs = FileSystem.get(conf);
				Path filePath =new Path(centroid);
				FSDataInputStream in = fs.open(filePath);
				String next="";
				String str="";
				  while(in!=null)
			    	 {
			    		   
			    		   next=in.readLine();
			    
			    		   if (next==null)
			    			  break;
			    			  System.out.print(next);
			    		   str =str.concat(next).concat("\n");
			    	 }
				  str.concat("\n");
				
				in.close();
				FSDataOutputStream out = fs.create(filePath);
				out.writeBytes(str);
				out.close();
				
				
			 
			count++;
			
		  TRUE = getstatus(conf, centroid);
		  
			}
   	 
     	FileSystem fs = FileSystem.get(conf);
	    Path filePath =new Path(centroid);
        	       
		FSDataInputStream in = fs.open(filePath);
		String next="";
		String str="";
		  while(in!=null)
	    	 {
	    		   
	    		   next=in.readLine();
	    
	    		   if (next==null)
	    			  break;
	    			  System.out.print(next);
	    		   str =str.concat(next).concat("\n");
	    	 }
		  str.concat("\n++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
		  str.concat(String.valueOf(count));
		  str.concat("\n++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
		in.close();

   	    	 
	      }
	
	@SuppressWarnings("deprecation")
	private static Boolean getstatus(Configuration conf ,String filePath) throws IOException
	{
		
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(filePath);
		FSDataInputStream in = fs.open(path);
		String str1="";
		String str2="";
		String temp="";
		while(in!=null)
		{
			   
 		   temp=in.readLine();
 		  
 		   if (temp==null)
 			  break; 
 		   
 		   str2 =str1;
 		   str1=temp;
 		   
		
		}
		in.close();
		//System.out.println("string 1 "+str1 + " string 2 "+str2 );
	    if( str1.equals(str2))
	    {
	    	return false;
	    }
		
		return true;
	}
	
	
	private static void deleteFolder(Configuration conf, String folderPath ) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(folderPath);
		if(fs.exists(path)) {
			fs.delete(path,true);
		}
	}
	public static void createFile(Configuration conf ,String filePath) throws IOException
	{
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(filePath);
		if(fs.exists(path)) {
			fs.delete(path,true);
		}
		FSDataOutputStream out = fs.create(path);
		out.writeBytes("50 500 5000 ");
		out.close();
		//FSDataInputStream in =fs.open(path);
		//System.out.println(in.readLine());
		//in.close();
	}
}
