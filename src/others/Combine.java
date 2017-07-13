package others;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * simply add values
 */
public class Combine extends Reducer<Text, IntWritable, Text, IntWritable>{
	private int sum;
	private IntWritable output = new IntWritable();
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}	
		output.set(sum);
		context.write(key, output);
		
	}

}
