import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Link implements WritableComparable<Link> {
    private String from;
    private String to;

    public void set(String from, String to) {
        this.from = from;
        this.to = to;
    }

    public String getFrom() {
        return from;
    }

    public String getTo() {
        return to;
    }


    public int compareTo(Link l) {
        int i = from.compareTo(l.from);
        if (i == 0) {
            return to.compareTo(l.to);
        }
        return i;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(from);
        out.writeUTF(to);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        from = in.readUTF();
        to = in.readUTF();
    }

    @Override
    public int hashCode() {
        return Math.abs(from.hashCode());
    }

    @Override
    public String toString() {
        return from + " " + to;
    }
}
