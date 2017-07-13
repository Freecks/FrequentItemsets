package mappers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapFirstMining extends Mapper<LongWritable, Text, Text, IntWritable>{
	private String line;
	private IntWritable output = new IntWritable();
	private Text word = new Text();
	private List<Integer> tokens;
	private StringTokenizer tokenizer;
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		line = value.toString();
		output.set(Integer.parseInt(line.substring(0, line.indexOf('\t'))));
		line = line.substring(line.indexOf('\t')+1);
			
		tokens = new ArrayList<Integer>();      
        tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            tokens.add(Integer.valueOf(tokenizer.nextToken()));
        }
        
        //we will count 2-itemsets, so we create all 2-itemsets from transactions
        for(int i = 0; i<tokens.size(); i++){	
        	for(int j = (i+1); j<tokens.size(); j++){
        		word.set(new StringBuilder().append(tokens.get(i)).append(",").append(tokens.get(j)).toString());
        		context.write(word, output);
        	}
        }		
	}
}
