package stage.leo;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/*
 * Adding transactions to the same value string
 */
public class ReduceFirstIter extends TableReducer<Text, Text, ImmutableBytesWritable>{
	//private Text output = new Text();
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		StringBuilder out = new StringBuilder();
		for(Text s : values){
			out.append(s.toString()).append(" ");
		}
		String keyString = key.toString();
		String partition = keyString.substring(keyString.indexOf(":")+1);
		//output.set(out.toString().replaceAll("  ", " ")); // je ne vois pas l'interet
		//context.write(key, output);
		Put put = new Put(Bytes.toBytes(key.toString()));
		put.addColumn(Bytes.toBytes("f"), Bytes.toBytes(partition), Bytes.toBytes(out.toString().replaceAll("  ", " ")));
		context.write(null, put);
	}
}
