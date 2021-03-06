package mappers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapFreqClean extends Mapper<LongWritable, Text, Text, Text>{
	private Configuration conf;
	private Text word = new Text(), word2 = new Text();
	private String line;
	private List<String> l1 = new ArrayList<String>();
	
	@Override
    protected void setup(Context context) throws IOException, InterruptedException {
		conf = context.getConfiguration();
    	
		FileSystem fs = FileSystem.get(conf);
    	FileStatus [] status = fs.listStatus(new Path(conf.get("path")));
    	
    	//we store frequent k-itemsets
    	for (FileStatus file : status) {
    		Path p = file.getPath();
    		InputStreamReader ir = new InputStreamReader(fs.open(p));
    		BufferedReader data = new BufferedReader(ir);
    		while (data.ready()) {    		
    			line = data.readLine();
    			l1.add(line.substring(0, line.indexOf('\t')));
    		}
    	}
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		line = value.toString();
		if(l1.contains(line.substring(0,line.indexOf(':')))){			
			word.set(line.substring(0,line.indexOf('-')));
			word2.set(line.substring(line.indexOf('\t')+1));
			context.write(word, word2);
		}
	}
}
