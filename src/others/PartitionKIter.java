package others;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionKIter extends Partitioner<Text,Text>{
	private String str;
	private int num;
	
	@Override
	public int getPartition(Text key, Text value, int numReduceTasks){
		str = key.toString();
		num = Integer.valueOf(str.substring(1,str.indexOf('-')));
		
		if((num/numReduceTasks) % 2 == 0)	//we sort datas to send them into the right reducer, while keeping similar amount of data on each
			return num%numReduceTasks;
		else
			return numReduceTasks-1 - (num%numReduceTasks);
	}

}
