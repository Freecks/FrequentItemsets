package reducers;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Similar to a wordcount but we only write items with more occurences than a threshold parameter
 */
public class ReduceFrequentTokens extends Reducer<Text, IntWritable, Text, IntWritable>{	
	private int freq;
	private int sum;
	private IntWritable output = new IntWritable();
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		freq = conf.getInt("frequence", 0);	//support threshold parameter
	} 
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		if(sum > freq){
			output.set(sum);
			context.write(key, output);
		}
	}
	
}
