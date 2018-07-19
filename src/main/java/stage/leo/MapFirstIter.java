package stage.leo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class MapFirstIter extends Mapper<LongWritable, Text, Text, Text>{
	private Configuration conf;
	private Text keyOut = new Text(), output = new Text();
	private int lineCounter;
	private String line, size, mapperId;
	private HashSet<String> l1;
	private StringTokenizer tokenizer;
	
	
	
	
	@Override
    protected void setup(Context context) throws IOException, InterruptedException {
		conf = context.getConfiguration();
    	mapperId = conf.get("mapred.task.partition", "5");
    	lineCounter = 0;
    	
    	FileSystem fs = FileSystem.get(conf);
    	FileStatus [] status = fs.listStatus(new Path(conf.get("path")));
    	
    	//we store frequent 1-itemsets
    	l1 = new HashSet<String>();
    	for (FileStatus file : status) {
    		Path p = file.getPath();
    		InputStreamReader ir = new InputStreamReader(fs.open(p));
    		BufferedReader data = new BufferedReader(ir);
    		while (data.ready()) {    		
    			line = data.readLine();
    			l1.add(new String(line.substring(0, line.indexOf('\t'))));
    		}
    	}
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		line = value.toString();	
		size = line.substring(0, line.indexOf('\t'));
		line = line.substring(line.indexOf('\t')+1);
		tokenizer = new StringTokenizer(line);
		StringBuilder tempKey;
        while (tokenizer.hasMoreTokens()) {
            tempKey = new StringBuilder (tokenizer.nextToken());
    		if(l1.contains(tempKey.toString())){
    			keyOut.set(tempKey.append(":").append(mapperId).toString());
    			if(size.equals("1"))
    				output.set(new StringBuilder().append(lineCounter).toString());
    			else
    				output.set(new StringBuilder().append(lineCounter).append(".").append(size).toString());		
    			context.write(keyOut, output);        			
    		}
        }
		lineCounter ++;		
	}
}
