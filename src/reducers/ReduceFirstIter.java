package reducers;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Adding transactions to the same value string
 */
public class ReduceFirstIter extends Reducer<Text, Text, Text, Text>{
	private Text output = new Text();
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		StringBuilder out = new StringBuilder();
		for(Text s : values){
			out.append(s.toString()).append(" ");
		}
		output.set(out.toString().replaceAll("  ", " "));
		context.write(key, output);
	}
}
