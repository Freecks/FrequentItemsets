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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class MapFirstIter extends TableMapper<Text, Text>{
	private Configuration conf;
	private Text keyOut = new Text(), output = new Text();
	private int lineCounter;
	private String size, mapperId;
	private HashSet<String> l1;
	private StringTokenizer tokenizer;
	private static final byte[] one = Bytes.toBytes("1");
	
	
	
	
	@Override
    protected void setup(Context context) throws IOException, InterruptedException {
		conf = context.getConfiguration();
    	mapperId = conf.get("mapred.task.partition", "5");
    	lineCounter = 0;
    	
    	l1 = new HashSet<String>();
    	Configuration conf2 = HBaseConfiguration.create();
		Connection cc = ConnectionFactory.createConnection(conf2);
		try {
			TableName tableName = TableName.valueOf("freq");
			Table table = cc.getTable(tableName);
			Scan scan = new Scan();
			scan.addColumn(Bytes.toBytes("s"), Bytes.toBytes("1"));
			ResultScanner scanner = table.getScanner(scan);
			for(Result result : scanner) {
				l1.add(Bytes.toString(result.getRow()));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			cc.close();
		}
	}
	
	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
		size = Bytes.toString(value.getValue(Bytes.toBytes("f"), Bytes.toBytes("a")));
		tokenizer = new StringTokenizer(Bytes.toString(row.get()));
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
