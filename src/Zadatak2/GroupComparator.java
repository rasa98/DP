import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupComparator extends WritableComparator {
    protected GroupComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        CompositeKey ck1 = (CompositeKey) w1;
        CompositeKey ck2 = (CompositeKey) w2;
        return ck1.getOriginalKey().compareTo(ck2.getOriginalKey());
    }
}
