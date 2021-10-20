import java.io.IOException;
import java.util.StringTokenizer;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 
public class WordCount {
 
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
 
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
 
            StringTokenizer str = new StringTokenizer(value.toString());
 
            while (str.hasMoreTokens()) {
                String word = str.nextToken();
 
                context.write(new Text(word), new IntWritable(1));
            }
        }
    }
 
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
 
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable i : values) {
                sum += i.get();
            }
 
            context.write(key, new IntWritable(sum));
        }
    }
 
    public static void main(String[] args) throws Exception {
 
        if (args.length != 2) {
            System.err.println("Usage: WordCount <InPath> <OutPath>");
            System.exit(2);
        }
 
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "WordCount");
 
        job.setJarByClass(WordCount.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setNumReduceTasks(1);
 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
 
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}