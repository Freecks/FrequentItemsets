package stage.leo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompareFreqClean extends WritableComparator{
	private String key1, key2;
	private String i1, i2;

	public CompareFreqClean() {
		super(Text.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		key1 = w1.toString();
		key2 = w2.toString();
		if(key1.indexOf(':') == -1) {
			i1 = "0";
		}
		else {
			key1 = key1.substring(0, key1.indexOf(':'));
			i1 = "1";
		}
		if(key2.indexOf(':') == -1) {
			i2 = "0";
		}
		else {
			key2 = key2.substring(0, key2.indexOf(':'));
			i2 = "1";
		}
		
		if(key1.equals(key2))
			return i1.compareTo(i2);
		else
			return key1.compareTo(key2);
	}
	
}
