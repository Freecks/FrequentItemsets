package stage.leo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReduceKIter extends Reducer<Text, Text, Text, Text> {
	private int size, partition, sub, prefixA, prefixB;
	private String out = " ", toKey, line, value;
	private Text output = new Text(), keyout = new Text();
	private HashSet<String> toCompare;
	private HashMap<String,String> listA;
	private StringTokenizer tokenizer;
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		if(key.toString().contains("A")){	//for A flag : we store values
			line = key.toString();
			partition = Integer.parseInt(line.substring(line.indexOf('-')+1, line.indexOf(',')));
			sub = Integer.parseInt(line.substring(line.indexOf(':')+1));
			listA = new HashMap<String,String>();
			for(Text val : values){	//we store keys and values separately
				line = val.toString();
				listA.put(line.substring(0,line.indexOf('\t')),line.substring(line.indexOf('\t')+1));
			}
		}
		else{	//for B flag : we try to join each key with A block's keys
			line = key.toString();
			if(listA != null && Integer.parseInt(line.substring(1, line.indexOf('-'))) == partition && Integer.parseInt(line.substring(line.indexOf(':')+1)) == sub){	//this test if we have the good A block stored
				
				for(Text val : values){			
					line = val.toString();
					toKey = line.substring(0,line.indexOf('\t'));
					line = line.substring(line.indexOf('\t')+1);
					toCompare= new HashSet<String>();
					tokenizer = new StringTokenizer(line);
					while (tokenizer.hasMoreTokens()) {
						toCompare.add(tokenizer.nextToken());	//items to intersect if we can join keys
					}
					
					prefixB = Integer.parseInt(toKey.substring(0,toKey.indexOf(',')));
					
					for(String tempKeyA : listA.keySet()){	//for each key of A block
						prefixA = Integer.parseInt(tempKeyA.substring(0,tempKeyA.indexOf(',')));
						line = key.toString();
						if(prefixA < prefixB || Integer.parseInt(line.substring(line.indexOf('-')+1, line.indexOf(','))) != partition){	//if we can join
							size = 0;
							line = listA.get(tempKeyA);
							tokenizer = new StringTokenizer(line);
							while (tokenizer.hasMoreTokens()) {
								value = tokenizer.nextToken();
								if(toCompare.contains(value) && !out.contains(" " + value + " ")){	//we keep the intersection of values
									out+= value + " ";	//on l'ajoute à la sortie
									if(value.indexOf('.') != -1)
										size += Integer.parseInt(value.substring(value.indexOf(".")+1, value.length()));
									else
										size ++;
								}
							}								
							
							if(size>0){	//if intersetion isn't empty
								//we create the new itemset with it's size
								if(prefixA > prefixB)
									keyout.set(prefixB +  "," + tempKeyA + "-" + size);
								else
									keyout.set(prefixA + "," + toKey + "-" + size);
								output.set(out);
								context.write(keyout, output);
								output.set("");
							}
							out = " ";
						}   				
					}
				}
			}
		}
	}
}
