import java.io.*;
import java.util.*;
import java.lang.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FindMaximumInteger {
	
	public static class MyMapper extends Mapper<Object, Text, Text, IntWritable>{
		
		private Text word = new Text("Local Maximum Integer");
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			StringTokenizer i = new StringTokenizer(value.toString());
			int maximum_value = Integer.MIN_VALUE;
      
			while (i.hasMoreTokens()) {
				int current_value = Integer.parseInt(i.nextToken());
				if(current_value>maximum_value)
					maximum_value=current_value;
			}
			
			context.write(word, new IntWritable(maximum_value));
		}
	}

	public static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		
		private IntWritable final_result = new IntWritable();
		private Text word = new Text("Global Maximum Integer");
    
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			
			int maximum_integer = Integer.MIN_VALUE;
			for (IntWritable val : values) {
				if(val.get()>maximum_integer)
					maximum_integer=val.get();
			}
			
			final_result.set(maximum_integer);
			context.write(word, final_result);
		}
	}

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Find Largest Integer");
	    
	    job.setJarByClass(FindMaximumInteger.class);
	    job.setMapperClass(MyMapper.class);
	    job.setCombinerClass(MyReducer.class);
	    job.setReducerClass(MyReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}