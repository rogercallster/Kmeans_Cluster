package sample;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ClusterReducer 
extends Reducer<IntWritable,Text,IntWritable,Text> {
	
	 
	@SuppressWarnings("deprecation")
	public void reduce(IntWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		//System.out.println(key+"  \n");
		//System.out.println(key.toString());
		if (values != null)
		{
		int sum = 0,i=0;
		//System.out.println(key+"  ..................................");
		for (Text val : values) {
			i++;
			sum+=Integer.parseInt(val.toString().split("\\s+")[1]);
			context.write(key, val);
			
			//context.write(key, val);
		}
		
		int centroid= sum/i;
		
		FileSystem fs = FileSystem.get(context.getConfiguration());
		Path path = new Path("/centroid");
		FSDataInputStream in = fs.open(path);
		String next="";
		String str="";
		  while(in!=null)
	    	 {   
	    		   next=in.readLine();
	    
	    		   if (next==null)
	    			  break;
	    			  //System.out.println(next);
	    			  if (next.split("\\s+").length==3)
	    				  str =str+(next)+("\n");
	    			  else 
	    				  str =str+(next);
	    	 }
		  //System.out.println("new centroid "+centroid + " "+ str);
		  str= str+String.valueOf(centroid) + " ";
		//  System.out.println("  "+ str);
		in.close();
		
		FSDataOutputStream out = fs.create(path);
		out.writeBytes(str);
		out.close();
		
		
		/*File file =new File ("centroid");
		FileWriter fileWriter = new FileWriter(file,true);
		String write = String.valueOf(centroid+"\t");
		fileWriter.append(write);
		fileWriter.close();*/
		
		
		
		}

	    
	}
}
