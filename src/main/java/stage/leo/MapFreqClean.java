package stage.leo;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapFreqClean extends Mapper<Text, Text, Text, Text>{
	private Text word = new Text(), word2 = new Text();
	private String line;
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		line = key.toString();
		word.set(line.substring(0,line.indexOf(':')));
		word2.set(line.substring(line.indexOf('-')+1));
		context.write(word, word2);
		word.set(line.substring(0,line.indexOf('-')));
		context.write(word, value);
	}
}
