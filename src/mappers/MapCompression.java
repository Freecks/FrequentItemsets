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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapCompression extends Mapper<LongWritable, Text, Text, IntWritable>{
	private final static IntWritable one = new IntWritable(1);
	private Text keyOut = new Text();
    private StringTokenizer tokenizer;
    private String token;
    private List<Integer> l1;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {	  	
    	Configuration conf = context.getConfiguration();
    	
    	//we store all frequent items in a list
    	FileSystem fs = FileSystem.get(conf);
    	FileStatus [] status = fs.listStatus(new Path(conf.get("path")));
    	
    	l1 = new ArrayList<Integer>((int)conf.getLong("records", 50));
    	for (FileStatus file : status) {
    		Path p = file.getPath();
    		InputStreamReader ir = new InputStreamReader(fs.open(p));
    		BufferedReader data = new BufferedReader(ir);
    		while (data.ready()) {
    			String line = data.readLine();
    			l1.add(Integer.parseInt(line.substring(0, line.indexOf('\t'))));
    		}
    	}
    	
    } 
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	StringBuilder line = new StringBuilder();
        tokenizer = new StringTokenizer(value.toString());
        while (tokenizer.hasMoreTokens()) {
            token = tokenizer.nextToken();
            if(l1.contains(Integer.parseInt(token)))	//only keep frequent items
            	line.append(token).append(" ");	
        }
        if(line.length() > 3){ //we try not to keep transactions of 1 item (we can't build a 2-itemset with only 1 item)
        	keyOut.set(line.toString());
        	context.write(keyOut, one);		        	
        } 	
    }
}
