package stage.leo;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * just count frequency of transactions
 */
public class ReduceCompression extends TableReducer<Text, IntWritable, ImmutableBytesWritable>{
	private int sum;
	private IntWritable output = new IntWritable();
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}	
		//output.set(sum);
		//context.write(output, key);
		Put put = new Put(Bytes.toBytes(key.toString()));
		put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("a"), Bytes.toBytes("" + sum));
		context.write(null, put);
	}
}
