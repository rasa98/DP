import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

public class MyTokenizerMapper
        extends Mapper<Object, Text, Link, IntWritable> {
    
    private final static IntWritable one = new IntWritable(1);
    private Link link = new Link();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String delimiters = " ,.\"():;'[]";
        StringTokenizer itr = new StringTokenizer(value.toString(), delimiters);
        
        String lastToken = null;

        while (itr.hasMoreTokens()) {
            String currentToken = itr.nextToken().toLowerCase();

            if (lastToken != null) {
                // Link A -> B
                link.set(lastToken, currentToken);
                context.write(link, one);

                // Total for A (A -> *)
                link.set(lastToken, "*");
                context.write(link, one);
            }
            
            lastToken = currentToken;
        }
        // Note: No need to clear anything here because lastToken 
        // is a local variable that resets for every new line.
    }
}
