package stage.leo;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapKIter extends Mapper<LongWritable, Text, Text, Text> {
	private Configuration conf;
	private Text word = new Text(), word2 = new Text();
	private int splitSize, id;
	private String line;
	
	@Override
    protected void setup(Context context) throws IOException, InterruptedException {
		conf = context.getConfiguration();
		splitSize = Math.max(conf.getInt("mapreduce.input.fileinputformat.split.minsize", 0), Math.min(conf.getInt("mapreduce.input.fileinputformat.split.maxsize", 0), conf.getInt("dfs.blocksize", 0)));
	}
	
	/*
	 * we partition data into blocks (see annex for details)
	 */
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		line = value.toString();
		line = line.substring(line.indexOf(','), line.indexOf('\t'));
		id = (int)key.get()/splitSize;
		word.set("A"+id+"-"+id+line);
		word2.set(value.toString());
		context.write(word, word2);
		for(int i = 0; i<=id; i++){
			word.set("B"+i+"-"+id+line);
			context.write(word, word2);	
		}
	}

}
