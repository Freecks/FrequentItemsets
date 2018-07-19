package stage.leo;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/*
 * Similar to a wordcount
 */
public class MapFrequentTokens extends Mapper<LongWritable, Text, Text, IntWritable> {	
	private final static IntWritable one = new IntWritable(1);
	private Text keyOut = new Text();
	private StringTokenizer tokenizer;
	int splitSize;
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		tokenizer = new StringTokenizer(value.toString());		
        while (tokenizer.hasMoreTokens()) {
        	keyOut.set(tokenizer.nextToken());
            context.write(keyOut, one);
        }
		
	}
} 

