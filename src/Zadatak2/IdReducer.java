import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IdReducer
        extends Reducer<CompositeKey, Text, Text, Text> {
    private final Text originalKey = new Text();
    private final Text fileInfo = new Text();

    public void reduce(CompositeKey key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        StringBuilder formattedValues = new StringBuilder();
        for (Text t : values) {
            formattedValues.append(t.toString()).append(" ");
        }
        fileInfo.set(formattedValues.toString().strip());
        originalKey.set(key.getOriginalKey());
        context.write(originalKey, fileInfo);
    }
}
