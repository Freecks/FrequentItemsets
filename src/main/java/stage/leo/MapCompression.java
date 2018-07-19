package stage.leo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
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
    private HashSet<String> l1;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {	  	
    	Configuration conf = context.getConfiguration();
    	
    	//we store all frequent items in a list
    	FileSystem fs = FileSystem.get(conf);
    	FileStatus [] status = fs.listStatus(new Path(conf.get("path")));
    	
    	l1 = new HashSet<String>();
    	for (FileStatus file : status) {
    		Path p = file.getPath();
    		InputStreamReader ir = new InputStreamReader(fs.open(p));
    		BufferedReader data = new BufferedReader(ir);
    		while (data.ready()) {
    			String line = data.readLine();
    			l1.add(new String(line.substring(0, line.indexOf('\t'))));
    		}
    	}    	
    } 
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	StringBuilder line = new StringBuilder();
        tokenizer = new StringTokenizer(value.toString());
        int i=0; //Mostafa
        while (tokenizer.hasMoreTokens()) {
            token = tokenizer.nextToken();
            if(l1.contains(token)){	//only keep frequent items
            	line.append(token).append(" ");
            	i++;
            }
        }
        if( i > 1 ){ // Mostafa
        	keyOut.set(line.toString());
        	context.write(keyOut, one);		        	
        } 	
    }
}
