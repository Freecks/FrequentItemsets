package stage.leo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapSecIter extends Mapper<LongWritable, Text, Text, Text>{
	private Configuration conf;
	private Text word = new Text(), word2 = new Text();
	private int mapperId;
	private String line;
	
	@Override
    protected void setup(Context context) throws IOException, InterruptedException {
		conf = context.getConfiguration();
		mapperId = Integer.parseInt(conf.get("mapred.task.partition", "5"));
	}
	
	/*
	 * we partition data into blocks (see annex for details)
	 */
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		line = value.toString();
		line = line.substring(line.indexOf(':'), line.indexOf('\t'));
		word.set("A"+mapperId+"-"+mapperId+line);
		word2.set(value.toString());
		context.write(word, word2);
		for(int i = 0; i<=mapperId; i++){
			word.set("B"+i+"-"+mapperId+line);
			context.write(word, word2);	
		}
	}
}
