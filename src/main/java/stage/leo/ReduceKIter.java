package stage.leo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;
import java.util.HashSet;

import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceKIter extends Reducer<Text, Text, Text, BytesWritable> {
	private int size, partition, sub, prefixA, prefixB;
	private String toKey;
	private Text keyout = new Text();
	private HashMap<String,HashSet<String>> listA;
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		if(key.toString().contains("A")){	//for A flag : we store values
			String line = key.toString();
			partition = Integer.parseInt(line.substring(line.indexOf('-')+1, line.indexOf(':')));
			sub = Integer.parseInt(line.substring(line.indexOf(':')+1));
			listA = new HashMap<String,HashSet<String>>();
			for(Text val : values){	//we store keys and values separately
				line = val.toString();
				String s = new String(line.substring(line.indexOf('\t')+1));
				int len = s.length();
			    byte[] data = new byte[(len+1) / 3];
			    for (int i = 0; i < len; i += 3) {
			        data[i / 3] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
			                             + Character.digit(s.charAt(i+1), 16));
			    }
				listA.put(new String(line.substring(0,line.indexOf('\t'))),(HashSet<String>)SerializationUtils.deserialize(data));
			}
		}
		else{	//for B flag : we try to join each key with A block's keys
			String line = key.toString();
			if(listA != null && Integer.parseInt(line.substring(1, line.indexOf('-'))) == partition && Integer.parseInt(line.substring(line.indexOf(':')+1)) == sub){	//this test if we have the good A block stored
				
				for(Text val : values){			
					line = val.toString();
					toKey = new String(line.substring(0,line.indexOf('\t')));
					String s = new String(line.substring(line.indexOf('\t')+1));
					int len = s.length();
				    byte[] data = new byte[(len+1) / 3];
				    for (int i = 0; i < len; i += 3) {
				        data[i / 3] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
				                             + Character.digit(s.charAt(i+1), 16));
				    }
				    HashSet<String> toCompare = (HashSet<String>)SerializationUtils.deserialize(data);
					prefixB = Integer.parseInt(toKey.substring(0,toKey.indexOf(',')));
					
					for(String tempKeyA : listA.keySet()){	//for each key of A block
						prefixA = Integer.parseInt(tempKeyA.substring(0,tempKeyA.indexOf(',')));
						line = key.toString();
						if(tempKeyA.substring(tempKeyA.indexOf(',')).equals(toKey.substring(toKey.indexOf(','))) && (prefixA < prefixB || Integer.parseInt(line.substring(line.indexOf('-')+1, line.indexOf(':'))) != partition)){	//if we can join
							size = 0;
							HashSet<String> setA = (HashSet<String>) listA.get(tempKeyA).clone();
							setA.retainAll(toCompare);
							for(String str : setA) {
								if(str.indexOf('.') != -1)
									size += Integer.parseInt(str.substring(str.indexOf(".")+1));
								else
									size ++;
							}						
							
							if(size>0){	//if intersetion isn't empty
								//we create the new itemset with it's size
								if(prefixA > prefixB)
									keyout.set(prefixB +  "," + tempKeyA + "-" + size);
								else
									keyout.set(prefixA + "," + toKey + "-" + size);
								context.write(keyout, new BytesWritable(SerializationUtils.serialize(setA)));
							}
						}   				
					}
				}
			}
		}
	}
}
