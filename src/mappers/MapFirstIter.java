package mappers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MapFirstIter extends Mapper<LongWritable, Text, Text, Text>{
	private Configuration conf;
	private Text keyOut = new Text(), output = new Text();
	private int lineCounter, mapperId, tasks, size;
	private String line;
	private List<String> l1;
	private StringTokenizer tokenizer;
	private List<Integer> tokens;
	
	@Override
    protected void setup(Context context) throws IOException, InterruptedException {
		conf = context.getConfiguration();
		tasks = Integer.parseInt(conf.get("mapred.map.tasks"));
    	mapperId = Integer.parseInt(conf.get("mapred.task.partition", "5"));
    	lineCounter = mapperId;
    	
    	FileSystem fs = FileSystem.get(conf);
    	FileStatus [] status = fs.listStatus(new Path(conf.get("path")));
    	
    	//we store frequent 2-itemsets
    	l1 = new ArrayList<String>((int)conf.getLong("records", 50));
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
		size = Integer.valueOf(line.substring(0, line.indexOf('\t')));
		line = line.substring(line.indexOf('\t')+1);
		tokens = new ArrayList<Integer>();
		tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            tokens.add(Integer.valueOf(tokenizer.nextToken()));
        }
        
        for(int i = 0; i<tokens.size(); i++){	//we transform horizontal base into vertical base, we have to create unique ids for transactions
        	for(int j = i+1; j<tokens.size(); j++){
        		StringBuilder tempKey = new StringBuilder().append(tokens.get(i)).append(",").append(tokens.get(j));
        		if(l1.contains(tempKey.toString())){
        			keyOut.set(tempKey.append(":").append(mapperId).toString());
        			if(size == 1)
        				output.set(new StringBuilder().append(lineCounter).toString());
        			else
        				output.set(new StringBuilder().append(lineCounter).append(".").append(size).toString());		
        			context.write(keyOut, output);        			
        		}
        	}
        }
		lineCounter += tasks;		
	}
}
