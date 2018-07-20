package stage.leo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapGetFreq extends Mapper<LongWritable, Text, Text, IntWritable>{
	private String line;
	private Text word = new Text();
	private IntWritable val = new IntWritable();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		line = value.toString();
		word.set(line.substring(0,line.indexOf(':')));	//we sort data with their keys; excluding partition
		val.set(Integer.parseInt(line.substring(line.indexOf('-')+1, line.indexOf('\t'))));
		context.write(word, val);
	}
}
