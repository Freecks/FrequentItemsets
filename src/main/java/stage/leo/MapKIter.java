package stage.leo;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class MapKIter extends Mapper<Text, Text, Text, Text> {
	private Text word = new Text();
	private int id;
	private String line;
	
	@Override
    protected void setup(Context context) throws IOException, InterruptedException {
		id = Integer.parseInt(context.getConfiguration().get("mapreduce.task.partition"));
	}
	
	/*
	 * we partition data into blocks (see annex for details)
	 */
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		line = key.toString();
		line = new String(line.substring(line.indexOf(':')));
		word.set("A"+id+"-"+id+line);
		context.write(word, new Text(key + "\t" + value));
		for(int i = 0; i<=id; i++){
			word.set("B"+i+"-"+id+line);
			context.write(word, new Text(key + "\t" + value));
		}
	}
}
