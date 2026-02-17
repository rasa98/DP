import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyIntSumReducer
        extends Reducer<Link, IntWritable, Link, DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();
    private int freq = 0;

    public void reduce(Link key, Iterable<IntWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            int i = val.get();
            sum += i;

        }
        String to = key.getTo();
        if (to.equals("*")) {
            freq = sum;
        } else {
            result.set((double) sum / (double) freq);
            context.write(key, result);
        }
    }
}
