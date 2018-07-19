package stage.leo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class ReduceFreqClean extends Reducer<Text, Text, Text, Text>{
	private String frequent;
	private final static Text output = new Text();
	private int freq;
	private MultipleOutputs<Text, Text> out;
	private Configuration conf;
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		conf = context.getConfiguration();
		out = new MultipleOutputs<Text,Text>(context);
		freq = conf.getInt("frequence", 0);	//support threshold parameter
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String line = key.toString();
		if(line.indexOf(':') == -1) {
			int size = 0;
			for(Text s : values) {
				size += Integer.parseInt(s.toString());
			}
			if(size >= freq) {
				frequent = line;
				out.write("dir", key, size+"", "frequent/part");
				context.write(output, output);
			}
		}			
		else {
			if(line.substring(0, line.indexOf(':')).equals(frequent)) {
				for(Text s : values) {
					line = key.toString();
					out.write("dir", key, s, "transaction/part");
				}
			}
		}
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException{
		out.close();
	}
	
}
