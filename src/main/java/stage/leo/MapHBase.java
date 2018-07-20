package stage.leo;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

public class MapHBase extends TableMapper<Text, Text>{
	
	@Override
    protected void setup(Context context) throws IOException, InterruptedException {
		System.out.println(context.getConfiguration().get("mapreduce.task.partition"));
	}

	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
		//System.out.println(context.getConfiguration().getInt("hbase.mapreduce.tableinput.mappers.per.region", 0));
		//System.out.println(context.getConfiguration().get("mapreduce.task.partition") + " : " + Bytes.toString(row.get()));
	}
}
