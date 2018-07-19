package stage.leo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompareSecIter extends WritableComparator {
	private String key1, block1, sub1, key2, block2, sub2;
	
	public CompareSecIter() {
		super(Text.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		key1 = w1.toString();
		block1 = key1.substring(1, key1.indexOf('-'));
		sub1 = key1.substring(key1.indexOf(':'));
		key2 = w2.toString();
		block2 = key2.substring(1, key2.indexOf('-'));
		sub2 = key2.substring(key2.indexOf(':'));
		
		if(block1.equals(block2)){	//we compare keys first
			if(sub1.equals(sub2)){
				return key1.compareTo(key2);	//for same keys, we compare the block flag, to have the A block first		
			}
			return sub1.compareTo(sub2);
		}
		
		
		return key1.substring(1).compareTo(key2.substring(1));
	}

}
