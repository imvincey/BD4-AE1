import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class KVPComparator extends WritableComparator{
	protected KVPComparator() {
		super(KeyValuePair.class, true);
	}
	
	@SuppressWarnings("rawtypes")
    @Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		KeyValuePair k1 = (KeyValuePair)w1;
		KeyValuePair k2 = (KeyValuePair)w2;
         
        int result = -1 * k1.getValue().compareTo(k2.getValue());
        if(0 == result) {
            result = k1.getKey().compareTo(k2.getKey());
        }
        return result;
    }
}