package sample;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ClusterMapper extends Mapper<Object, Text, IntWritable, Text>{

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		//System.out.println("from mapper "+ value);
		String[] currentCentroids  =	getCentroid(context.getConfiguration(),"/centroid");
		
		int border1=(Integer.parseInt(currentCentroids[0])+Integer.parseInt(currentCentroids[1]))/2;
		int border2=(Integer.parseInt(currentCentroids[1])+Integer.parseInt(currentCentroids[2]))/2;
		String itr = value.toString();
		//System.out.println(itr);
		String[] strArr= itr.split("\\s+");
		IntWritable centroid1= new IntWritable(Integer.parseInt(currentCentroids[0]));
		IntWritable centroid2= new IntWritable(Integer.parseInt(currentCentroids[1]));
		IntWritable centroid3= new IntWritable(Integer.parseInt(currentCentroids[2]));
			if (Integer.parseInt(strArr[1])< border1)
			{
			  context.write(centroid1,value);	
			}
			else if (Integer.parseInt(strArr[1])< border2)
			{
				context.write(centroid2,value);
			}
			else
			{
				context.write(centroid3,value);
			}
	}
	
	@SuppressWarnings("deprecation")
	public String[] getCentroid(Configuration conf,String filePath) throws IOException
	{
	
		FileSystem fs = FileSystem.get(conf);
	    Path path = new Path(filePath);
	    String str ="";
	    if(fs.exists(path)) {
	    	 FSDataInputStream in = fs.open (path);
	    	 String next=in.readLine();
	    	str =next;
	    	   while(in!=null)
	    	 {
	    		   next=in.readLine();
	    		  
	    		   if (next==null)
	    			  break; 
	    		   str =next;
	    	 }
	    	 in.close();
	    	}
	     //System.out.println(str);
	     String [] ret =  str.split("\\s+");
	    	     return ret;
	}
	
}
