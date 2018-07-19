package stage.leo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionKIter extends Partitioner<Text,Text>{
	private String str;
	private int num;
	
	@Override
	public int getPartition(Text key, Text value, int numReduceTasks){
		str = key.toString();
		//num = Integer.parseInt(str.substring(1,str.indexOf('-')));
		//String s = str.substring(str.indexOf(',')+1, str.indexOf(':')).replaceAll(",", "");		
		//return Math.abs(s.hashCode())%numReduceTasks;
		return Integer.parseInt(str.substring(str.indexOf(':')+1))%numReduceTasks;
	}

}
