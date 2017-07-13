import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import mappers.MapFrequentTokens;
import mappers.MapCompression;
import mappers.MapFirstIter;
import mappers.MapFirstMining;
import mappers.MapGetFreq;
import mappers.MapFreqClean;
import mappers.MapKIter;

import others.CompareKIter;
import others.PartitionKIter;
import others.Combine;

import reducers.ReduceFrequentTokens;
import reducers.ReduceCompression;
import reducers.ReduceFirstIter;
import reducers.ReduceKIter;

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
		FileOutputFormat.setOutputPath(job, new Path(args[1]+"/frequent/1"));
		
		return job;
	}
	
	/*
	 * Remove each unfrequent items from transactions and merge similar transactions while adding frequencies
	 * Requirements : <output-path>/frequent/1 && <input-path>
	 * Results output : <output-path>/compressed
	 */
	private static Job setupJobCompression(Configuration conf, String[] args, long records) throws Exception{
		conf.set("path", args[1] + "/frequent/1");
		conf.setLong("records", records);
		
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
	 * Mine frequent 2-itemsets from compressed database
	 * Requirements : <output-path>/compressed
	 * Results output : <output-path>/frequent/2
	 */
	private static Job setupJobFirstMining(Configuration conf, String[] args) throws Exception{	
		Job job = Job.getInstance(conf, "Mining frequent 2-itemsets");
		
		job.setJarByClass(App.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(MapFirstMining.class);
		job.setCombinerClass(Combine.class);
		job.setReducerClass(ReduceFrequentTokens.class);
		
		job.setNumReduceTasks(Integer.parseInt(args[4]));
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[1]+"/compressed"));
		FileOutputFormat.setOutputPath(job, new Path(args[1]+"/frequent/2"));
		
		
		
		return job;
	}
	
	/*
	 * Create a vertical database containing frequent 2-itemsets associated with the list of their transactions
	 * Requirements : <output-path>/frequent/2 && <output-path>/compressed
	 * Results output : <output-path>/final/2
	 */
	private static Job setupJobFirstIteration(Configuration conf, String[] args, long records) throws Exception{		
        conf.set("path", args[1] + "/frequent/2");
        conf.setLong("records", records);
		
		Job job = Job.getInstance(conf, "Mining 2-itemsets");
		
		job.setJarByClass(App.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(MapFirstIter.class);
		job.setCombinerClass(ReduceFirstIter.class);
		job.setReducerClass(ReduceFirstIter.class);
		
		job.setNumReduceTasks(Integer.parseInt(args[4]));
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[1]+"/compressed"));		
		FileOutputFormat.setOutputPath(job, new Path(args[1]+"/final/2"));
		
		return job;
	}
	
	/*
	 * Create all possibly frequent k-itemsets (not only frequent but also some that can be and we don't know yet if the are or not)
	 * Requirements : <output-path>/final/<k-1>
	 * Results output : <output-path>/finalNoSize/<k>
	 */
	private static Job setupJobKIter(Configuration conf, String[] args, int iteration) throws Exception{
		
		Job job = Job.getInstance(conf, "Mining k-itemsets");
		
		job.setJarByClass(App.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(MapKIter.class);
		job.setPartitionerClass(PartitionKIter.class);
		job.setSortComparatorClass(CompareKIter.class);
		job.setReducerClass(ReduceKIter.class);
		
		job.setNumReduceTasks(Integer.parseInt(args[4]));
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[1]+"/final/" + iteration));
		FileOutputFormat.setOutputPath(job, new Path(args[1]+"/finalNoSize/" + (iteration+1)));
		
		
		return job;
	}
	
	/*
	 * Mine frequent k-itemsets from k-itemsets parts 
	 * Requirements : <output-path>/finalNoSize/<k>
	 * Results output : <output-path>/frequent/<k>
	 */
	private static Job setupJobMiningFrequent(Configuration conf, String[] args, int iteration) throws Exception{
		conf.setInt("frequence", Integer.parseInt(args[2]));
		
		Job job = Job.getInstance(conf, "Mining frequent k-itemsets");
		
		job.setJarByClass(App.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(MapGetFreq.class);
		job.setCombinerClass(Combine.class);
		job.setReducerClass(ReduceFrequentTokens.class);
		
		job.setNumReduceTasks(Integer.parseInt(args[4]));
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[1]+"/finalNoSize/" + iteration));
		FileOutputFormat.setOutputPath(job, new Path(args[1]+"/frequent/" + iteration));
		
		return job;
	}

	/*
	 * Remove all unfrequent canndidates k-itemsets
	 * Requirements : <output-path>/finalNoSize/<k> && <output-path>/frequent/<k>
	 * Results output : <output-path>/final/<k>
	 */
	private static Job setupJobCleaningFrequent(Configuration conf, String[] args, int iteration) throws Exception{
		conf.set("path", args[1] + "/frequent/" + iteration);
		
		Job job = Job.getInstance(conf, "Cleaning unfrequent k-itemsets");
		
		job.setJarByClass(App.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(MapFreqClean.class);
		
		job.setNumReduceTasks(Integer.parseInt(args[4]));
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[1]+"/finalNoSize/" + iteration));
		FileOutputFormat.setOutputPath(job, new Path(args[1]+"/final/" + iteration));
		
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
		
		System.out.println("Ils y a " + lines.size() + " items fréquents");	//affichage
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
		
		long startTime = System.currentTimeMillis();
		int exitCode = ToolRunner.run(new Configuration(), new App(), args);
		long endTime = System.currentTimeMillis();
		
		System.out.println("Total time : " + (endTime - startTime)/1000 + " seconds.");		
		System.exit(exitCode);

	}

	
	@Override
	public int run(String[] args) throws Exception {

		//compressing database with only frequent items and transaction frequencies
		Job job = setupJobFrequentTokens(this.getConf(), args);	
		job.waitForCompletion(true);
		
		long records = job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_OUTPUT_RECORDS").getValue();
		
		job = setupJobCompression(this.getConf(), args, records);
		job.waitForCompletion(true);
		
		//mining frequent 2-itemsets
		job = setupJobFirstMining(this.getConf(), args);
		job.waitForCompletion(true);
		
		records = job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_OUTPUT_RECORDS").getValue();
		
		job = setupJobFirstIteration(this.getConf(), args, records);
		job.waitForCompletion(true);
		
		//mining frequent k-itemsets, from 3 to n
		int iter = 2;
		
		while(true){
			job = setupJobKIter(this.getConf(), args, iter);
			job.waitForCompletion(true);
			
			records = job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_OUTPUT_RECORDS").getValue();
			
			if(records == 0) break;	//if there's no results, don't start another job
			
			job = setupJobMiningFrequent(this.getConf(), args, iter+1);
			job.waitForCompletion(true);
			
			records = job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_OUTPUT_RECORDS").getValue();
			
			if(records == 0) break;	//if there's no results, don't start another job
			
			job = setupJobCleaningFrequent(this.getConf(), args, iter+1);
			job.waitForCompletion(true);
			
			iter++;
		}
		
		// delete useless directory
		Path output = new Path(args[1]+"/finalNoSize");
		FileSystem hdfs = FileSystem.get(this.getConf());
		hdfs.delete(output, true);

		hdfs = FileSystem.get(this.getConf());
		
		for(int i = 1; i<= iter; i++){
			FileUtil.copyMerge(hdfs, new Path(args[1]+"/frequent/" + i), hdfs, new Path(args[1]+"/concat/" + i), false, this.getConf(), null);
		}
		
		
		
		//display
		displayFinalData(this.getConf(), args);
		
		return 0;
	}





}
