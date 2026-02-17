import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, CompositeKey, Text> {

    private final CompositeKey ck = new CompositeKey();

    Text fileInfo = new Text();


    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // Get the name of the input file being processed
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

        // Get the byte offset of the current line
        // Track the start byte offset of the current line
        long lineStartByteOffset = key.get();

        // Calculate the byte offset of each word within the line. Ignore case
        String line = value.toString().toLowerCase();

        Pattern pattern = Pattern.compile("\\b[\\w']+\\b"); // This pattern includes letters, digits, and single quotes

        Matcher matcher = pattern.matcher(line);

        Set<String> uniqueTokens = new HashSet<>();
        while (matcher.find()) {
            String token = matcher.group();
            if (!uniqueTokens.contains(token)) {


                // Calculate the byte offset of each occurrence of the word within the line
                pattern = Pattern.compile("\\b" + Pattern.quote(token) + "\\b");
                Matcher innerMatcher = pattern.matcher(line);
                while (innerMatcher.find()) {
                    int wordStartIndex = innerMatcher.start();
                    long wordByteOffset = lineStartByteOffset + wordStartIndex;

                    String sortingValue = fileName + ":" + wordByteOffset;
                    fileInfo.set(sortingValue); // Use the word byte offset
                    ck.setOriginalKey(token);
                    ck.setSortingValue(sortingValue);
                    context.write(ck, fileInfo);
                }
                uniqueTokens.add(token);
            }
        }
    }
}
