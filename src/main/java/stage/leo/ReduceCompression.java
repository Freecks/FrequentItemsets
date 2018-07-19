package stage.leo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * just count frequency of transactions
 */
public class ReduceCompression extends Reducer<Text, IntWritable, IntWritable, Text>{
	private int sum;
	private IntWritable output = new IntWritable();
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}	
		output.set(sum);
		context.write(output, key);
	}
}
