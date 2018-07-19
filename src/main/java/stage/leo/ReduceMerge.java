package stage.leo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceMerge extends Reducer<Text, IntWritable, Text, IntWritable>{
	private int sum;
	private String path, itemset;
	Configuration conf;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		conf = context.getConfiguration();
		path = conf.get("path","");
	} 
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		FileSystem fs = FileSystem.get(conf);
		itemset = key.toString().replace(':', '-').trim();
		if(sum >= 2){
			FileUtil.copyMerge(fs, new Path(path+itemset), fs, new Path(path + "concat/" + itemset), true, conf, null);
			context.getCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_OUTPUT_RECORDS").increment(1);			
		}
		else {
			fs.delete(new Path(path+itemset), true);
		}
	}
}
