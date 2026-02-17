import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeKey implements WritableComparable<CompositeKey> {
    private String originalKey;
    private String sortingValue;

    public CompositeKey() {
    }

    public String getOriginalKey() {
        return originalKey;
    }

    public void setOriginalKey(String originalKey) {
        this.originalKey = originalKey;
    }

    public String getSortingValue() {
        return sortingValue;
    }

    public void setSortingValue(String sortingValue) {
        this.sortingValue = sortingValue;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(originalKey);
        out.writeUTF(sortingValue);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        originalKey = in.readUTF();
        sortingValue = in.readUTF();
    }

    @Override
    public int hashCode() {
        return originalKey.hashCode();
    }

    @Override
    public int compareTo(CompositeKey other) {
        // Implement comparison logic based on originalKey and sortingValue
        int keyComparison = originalKey.compareTo(other.originalKey);
        if (keyComparison != 0) {
            return keyComparison;
        }

        String[] sv1 = sortingValue.split(":");
        String[] sv2 = other.getSortingValue().split(":");

        int docIDComparison = sv1[0].compareTo(sv2[0]);
        if (docIDComparison != 0) {
            return docIDComparison;
        }
        int i1 = Integer.parseInt(sv1[1]);
        int i2 = Integer.parseInt(sv2[1]);
        return i1 - i2;
    }

    @Override
    public String toString() {
        return originalKey + " " + sortingValue;
    }
}
