import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AfterLine {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Zadatak1");
        job.setJarByClass(AfterLine.class);
        job.setMapperClass(MyTokenizerMapper.class);
        job.setReducerClass(MyIntSumReducer.class);
        job.setOutputKeyClass(Link.class);
        job.setOutputValueClass(IntWritable.class); // Zar ovde ne treba double???
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}