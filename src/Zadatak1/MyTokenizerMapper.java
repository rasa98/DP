import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

public class MyTokenizerMapper
        extends Mapper<Object, Text, Link, IntWritable> {

    private IntWritable sum = new IntWritable();
    private Link link = new Link();
    private Map<String, Integer> BafterA = new TreeMap<>();
    private Map<String, Integer> freq = new TreeMap<>();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String delimiters = " ,.\"():;'[]";
        StringTokenizer itr = new StringTokenizer(value.toString(), delimiters);
        while (itr.hasMoreTokens()) {
            String token = itr.nextToken().toLowerCase();
            for (Map.Entry<String, Integer> e : freq.entrySet()) {
                String tokenBefore = e.getKey();
                String tokenKey = tokenBefore + "," + token;
                int times = e.getValue();
                BafterA.put(tokenKey, BafterA.getOrDefault(tokenKey, 0) + times);
            }

            freq.put(token, freq.getOrDefault(token, 0) + 1);
        }

        for (Map.Entry<String, Integer> e : BafterA.entrySet()) {
            String[] ab = e.getKey().split(",");
            String a = ab[0];
            String b = ab[1];
            int localSum = e.getValue();

            link.set(a, b);
            sum.set(localSum);
            context.write(link, sum);
        }
        for (Map.Entry<String, Integer> e : freq.entrySet()) {
            String a = e.getKey();
            String b = "*";
            int localSum = e.getValue();

            link.set(a, b);
            sum.set(localSum);
            context.write(link, sum);
        }
        BafterA.clear();
        freq.clear();
    }
}
