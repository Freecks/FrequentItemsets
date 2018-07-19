package stage.leo;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class App extends Configured implements Tool{

	/*
	 * Mine frequent items from input path files
	 * Requirements : <input-path>
	 * Results output : <output-path>/frequent/1
	 */
	private static Job setupJobFrequentTokens(Configuration conf, String[] args) throws Exception{
		conf.setInt("frequence", Integer.parseInt(args[2]));
		
		Job job = Job.getInstance(conf, "Mining frequent 1-itemsets");

		job.setJarByClass(App.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(MapFrequentTokens.class);
		job.setCombinerClass(Combine.class);
		job.setReducerClass(ReduceFrequentTokens.class);
		
		job.setNumReduceTasks(Integer.parseInt(args[4]));
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]+"/final/1/frequent"));
		
		return job;
	}
	
	/*
	 * Remove each unfrequent items from transactions and merge similar transactions while adding frequencies
	 * Requirements : <output-path>/frequent/1 && <input-path>
	 * Results output : <output-path>/compressed
	 */
	private static Job setupJobCompression(Configuration conf, String[] args) throws Exception{
		conf.set("path", args[1] + "/final/1/frequent");
		
		Job job = Job.getInstance(conf, "Database compression");

		job.setJarByClass(App.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(MapCompression.class);
		job.setCombinerClass(Combine.class);
		job.setReducerClass(ReduceCompression.class);
		
		job.setNumReduceTasks(Integer.parseInt(args[4]));
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/compressed"));
		
		return job;	
	}
	
	/*
	 * Create a vertical database containing frequent 2-itemsets associated with the list of their transactions
	 * Requirements : <output-path>/frequent/1 && <output-path>/compressed
	 * Results output : <output-path>/final/1
	 */
	private static Job setupJobFirstIteration(Configuration conf, String[] args) throws Exception{		
        conf.set("path", args[1] + "/final/1/frequent");
		
		Job job = Job.getInstance(conf, "Mining 1-itemsets");
		
		job.setJarByClass(App.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		
		job.setMapperClass(MapFirstIter.class);
		job.setCombinerClass(CombineFirstIter.class);
		job.setReducerClass(ReduceFirstIter.class);
		
		job.setNumReduceTasks(Integer.parseInt(args[4]));
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[1]+"/compressed"));		
		FileOutputFormat.setOutputPath(job, new Path(args[1]+"/final/1/transaction"));
		
		return job;
	}
	
	private static Job setupJobSecondIteration(Configuration conf, String[] args) throws Exception{				
		Job job = Job.getInstance(conf, "Mining 2-itemsets");
		
		job.setJarByClass(App.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		
		job.setMapperClass(MapSecIter.class);
		job.setPartitionerClass(PartitionSecIter.class);
		job.setSortComparatorClass(CompareSecIter.class);
		job.setReducerClass(ReduceSecIter.class);
		
		job.setNumReduceTasks(Integer.parseInt(args[4]));
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[1]+"/final/1/transaction"));
		FileOutputFormat.setOutputPath(job, new Path(args[1]+"/temp/2"));
		
		return job;
	}
	
	/*
	 * Create all possibly frequent k-itemsets (not only frequent but also some that can be and we don't know yet if the are or not)
	 * Requirements : <output-path>/final/<k-1>
	 * Results output : <output-path>/finalNoSize/<k>
	 */
	private static Job setupJobKIter(Configuration conf, String[] args, int iteration) throws Exception{
		conf.set("path", args[1] + "/final/" + iteration + "/sizes/");
		
		Job job = Job.getInstance(conf, "Mining " + (iteration+1) + "-itemsets");
		
		job.setJarByClass(App.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		
		job.setMapperClass(MapKIter.class);
		job.setPartitionerClass(PartitionKIter.class);
		job.setSortComparatorClass(CompareKIter.class);
		job.setReducerClass(ReduceKIter.class);
		
		job.setNumReduceTasks(Integer.parseInt(args[4]));
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[1]+"/final/" + iteration + "/transaction/*"));
		FileOutputFormat.setOutputPath(job, new Path(args[1]+"/temp/" + (iteration+1)));
		
		return job;
	}
	
	/*
	 * Remove all unfrequent canndidates k-itemsets
	 * Requirements : <output-path>/finalNoSize/<k> && <output-path>/frequent/<k>
	 * Results output : <output-path>/final/<k>
	 */
	private static Job setupJobCleaningFrequent(Configuration conf, String[] args, int iteration) throws Exception{
		conf.setInt("frequence", Integer.parseInt(args[2]));
		
		
		Job job = Job.getInstance(conf, "Cleaning unfrequent " + iteration + "-itemsets");
		
		job.setJarByClass(App.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(MapFreqClean.class);
		job.setPartitionerClass(PartitionFreqClean.class);
		job.setSortComparatorClass(CompareFreqClean.class);
		job.setReducerClass(ReduceFreqClean.class);
		
		job.setNumReduceTasks(Integer.parseInt(args[4]));
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[1]+"/temp/" + iteration));
		MultipleOutputs.addNamedOutput(job, "dir", TextOutputFormat.class, Text.class, Text.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/final/" + iteration));
		
		return job;
	}
	
	/*
	 * Displaying datas
	 */
	private void displayFinalData(Configuration conf, String[] args) throws Exception{
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] status = fs.listStatus(new Path(args[1]+"/concat/"));
		
		String line;
		List<String> lines = new ArrayList<String>();	
		List<Integer> sizes = new ArrayList<Integer>();	
		
		for (int i=0;i<status.length;i++){
			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
			try {	    	
				line=br.readLine();
				while (line != null){
					lines.add(line.substring(0, line.indexOf("\t")));
					sizes.add(Integer.parseInt(line.substring(line.indexOf("\t")+1)));
					line = br.readLine();
				}
			} finally {
				br.close();
			}	    	
		}
		
		System.out.println("Il y a " + lines.size() + " items fréquents");	//affichage
		System.out.println(args[3] + " items les plus fréquents : ");
		int i = 0;
		while(i<Integer.parseInt(args[3]) && lines.size()>0){	//on cherche l'élément maximum du fichier de sortie puis on le supprime de la liste
			i++;
			int j = sizes.indexOf(Collections.max(sizes));
			System.out.println(lines.get(j) + "-" + sizes.get(j) +  " occurences");
			sizes.remove(j);
			lines.remove(j);
		}
		
	}
	
	public static void main(String[] args) throws Exception{
		
		/*PrintStream out = new PrintStream(new FileOutputStream("/home/lifoadm/output.txt"));
		PrintStream outErr = new PrintStream(new FileOutputStream("/home/lifoadm/outputErr.txt"));
		System.setOut(out);
		System.setErr(outErr);*/
		
		long startTime = System.currentTimeMillis();
		int exitCode = ToolRunner.run(new Configuration(), new App(), args);
		long endTime = System.currentTimeMillis();		
		
		System.out.println("Total time : " + (endTime - startTime)/1000 + " seconds.");		
		System.exit(exitCode);

	}
	    
	@Override
	public int run(String[] args) throws Exception {
		
		boolean verbose = true;

		//compressing database with only frequent items and transaction frequencies
		Job job = setupJobFrequentTokens(this.getConf(), args);	
		job.waitForCompletion(verbose);
				
		job = setupJobCompression(this.getConf(), args);
		job.waitForCompletion(verbose);
			
		//creating vertical database
		job = setupJobFirstIteration(this.getConf(), args);
		job.waitForCompletion(verbose);
		
		//mining frequent 2-itemsets
		job = setupJobSecondIteration(this.getConf(), args);
		job.waitForCompletion(verbose);
		
		//mining frequent k-itemsets, from 3 to n
		int iter = 2;
		
		while(true){
		
			job = setupJobCleaningFrequent(this.getConf(), args, iter);
			job.waitForCompletion(verbose);
			
			long records = job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_OUTPUT_RECORDS").getValue();
			
			if(records < (iter+1)) break;	//if there's not enough results, don't start another job

			job = setupJobKIter(this.getConf(), args, iter);
			job.waitForCompletion(verbose);
			
			records = job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_OUTPUT_RECORDS").getValue();
			
			if(records == 0) break;	//if there's no results, don't start another job
			
			iter++;
		}
		
		// delete useless directory
		//Path output = new Path(args[1]+"/finalNoSize");
		FileSystem hdfs = FileSystem.get(this.getConf());
		//hdfs.delete(output, true);

		hdfs = FileSystem.get(this.getConf());
		        
		
	    for(int i = 1; i<= iter; i++){
	    	try {
	    		FileUtil.copyMerge(hdfs, new Path(args[1]+"/final/" + i + "/frequent"), hdfs, new Path(args[1]+"/concat/" + i), false, this.getConf(), null);				
			} catch (FileNotFoundException e) {
			}
		}
		
		
		
		//display
		displayFinalData(this.getConf(), args);
		
		return 0;
	}





}
