package stage.leo;

import java.io.IOException;
import java.util.HashSet;

import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapSecIter extends Mapper<Text, Text, Text, Text>{
	private Configuration conf;
	private Text word = new Text();
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
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		line = key.toString();
		line = new String(line.substring(line.indexOf(':')));
		word.set("A"+mapperId+"-"+mapperId+line);
		context.write(word, new Text(key + "\t" + value));
		for(int i = 0; i<=mapperId; i++){
			word.set("B"+i+"-"+mapperId+line);
			context.write(word, new Text(key + "\t" + value));	
		}
	}
}
