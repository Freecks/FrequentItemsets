package stage.leo;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Adding transactions to the same value string
 */
public class ReduceFirstIter extends Reducer<Text, Text, Text, BytesWritable>{
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		HashSet<String> out = new HashSet<String>();
		for(Text s : values){
			out.addAll(Arrays.asList(s.toString().split(" ")));
		}
		BytesWritable output = new BytesWritable(SerializationUtils.serialize(out));
		context.write(key,output);
	}
}
