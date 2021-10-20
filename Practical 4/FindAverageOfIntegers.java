import java.io.*;
import java.util.*;
import java.lang.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FindAverageOfIntegers {
	
	public static class MyMapper extends Mapper<Object, Text, Text, FloatWritable>{
	  
	  private Text word = new Text("Average Of Integer");

	  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		  StringTokenizer i = new StringTokenizer(value.toString());
		  float local_total = 0;
          int count=0;
          
          while (i.hasMoreTokens()) {
        	  float current_value = Float.parseFloat(i.nextToken());
        	  local_total+= current_value;
        	  count++;
          }
          
          float local_average =local_total/count;
          context.write(word, new FloatWritable(local_average));
	  }
	}

	public static class MyReducer extends Reducer<Text,FloatWritable,Text,FloatWritable> {
		
		private FloatWritable final_result = new FloatWritable();
	    private Text word = new Text("Global Avergae Of Integer");
	    
	    public void reduce(Text key, Iterable<FloatWritable> values,Context context) throws IOException, InterruptedException {
	    	
	    	float total = 0;
   	        int count=0;
	      
   	        for (FloatWritable val : values) {
   	        	
   	        	total+=val.get();
   	        	count++;
	        }
   	        
	       float global_average = total/count;
	       final_result.set(global_average);
	       context.write(word, final_result);
	    }
	}

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Find Average Of Integer");
	    
	    job.setJarByClass(FindMaximumInteger.class);
	    job.setMapperClass(MyMapper.class);
	    job.setCombinerClass(MyReducer.class);
	    job.setReducerClass(MyReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(FloatWritable.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}