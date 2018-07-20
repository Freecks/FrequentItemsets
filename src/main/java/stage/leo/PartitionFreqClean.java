package stage.leo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionFreqClean extends Partitioner<Text,Text>{
	private String val;
	
	@Override
	public int getPartition(Text key, Text value, int numReduceTasks)
	{
		val = key.toString();
		
		if(val.indexOf(':') != -1)
			return Math.abs(val.substring(0, val.indexOf(':')).hashCode() % numReduceTasks);	//on aiguille chaque clé vers le même réducer, indépendemment du flag;
		else
			return Math.abs(val.hashCode() % numReduceTasks);
	}

}
