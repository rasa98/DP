import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SimilarTweets {

    public static class DistanceSimilarityMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

        IntWritable dist = new IntWritable();
        IntWritable one = new IntWritable(1);
        private String customString;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            customString = conf.get("custom.string"); // Retrieve the custom string from configuration
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            int calcDist = DamLeven.calculateDistance(line, customString);
            dist.set(calcDist);
            context.write(dist, one);
        }
    }



    public static class DistanceSimilarityReducer
            extends Reducer<IntWritable,IntWritable,IntWritable, IntWritable> {
        IntWritable sum = new IntWritable();
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int localSum = 0;
            for (IntWritable num : values) {
                localSum += num.get();
            }
            sum.set(localSum);
            context.write(key, sum);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("custom.string", args[3]); // Set the custom string in the configuration
        Job job = Job.getInstance(conf, args[0]);
        job.setJarByClass(SimilarTweets.class);
        job.setMapperClass(DistanceSimilarityMapper.class);
        job.setReducerClass(DistanceSimilarityReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}