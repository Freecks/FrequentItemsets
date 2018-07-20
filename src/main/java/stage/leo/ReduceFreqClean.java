package stage.leo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class ReduceFreqClean extends Reducer<Text, Text, Text, Text>{
	private String frequent, path, pathSize;
	private MultipleOutputs<Text, Text> out;
	private Configuration conf;
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		conf = context.getConfiguration();
		path = conf.get("path");
		out = new MultipleOutputs<Text,Text>(context);
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String line = key.toString();
		if(line.indexOf(':') == -1)
			frequent = line;
		else {
			if(line.substring(0, line.indexOf(':')).equals(frequent)) {
				for(Text s : values) {
					line = key.toString();
					String dir = line.substring(line.indexOf(',')+1).replace(":", "-");
					//context.write(key, s);
					out.write("dir", key, s, "raw/" + dir + "/part");
					out.write("dir", dir,new Text(),path);
				}
			}
		}
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException{
		out.close();
	}
	
}
