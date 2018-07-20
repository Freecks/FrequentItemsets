package stage.leo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormatBase;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
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
		
		Configuration conf2 = HBaseConfiguration.create();
		Connection cc = ConnectionFactory.createConnection(conf2);
		try {
			Admin a = cc.getAdmin();
			TableName tableName = TableName.valueOf("freq");
			if(!a.tableExists(tableName)) {
				HTableDescriptor table = new HTableDescriptor(tableName);
				HColumnDescriptor family = new HColumnDescriptor(Bytes.toBytes("s"));
				table.addFamily(family);
				a.createTable(table);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			cc.close();
		}
		
		Job job = Job.getInstance(conf, "Mining frequent 1-itemsets");

		job.setJarByClass(App.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setMapperClass(MapFrequentTokens.class);
		job.setCombinerClass(Combine.class);
		//job.setReducerClass(ReduceFrequentTokens.class);
		
		job.setNumReduceTasks(Integer.parseInt(args[4]));
		
		job.setInputFormatClass(TextInputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//FileOutputFormat.setOutputPath(job, new Path(args[1]+"/frequent/1"));
		
		TableMapReduceUtil.initTableReducerJob("freq", ReduceFrequentTokens.class, job);
		
		return job;
	}
	
	/*
	 * Remove each unfrequent items from transactions and merge similar transactions while adding frequencies
	 * Requirements : <output-path>/frequent/1 && <input-path>
	 * Results output : <output-path>/compressed
	 */
	private static Job setupJobCompression(Configuration conf, String[] args) throws Exception{
		//conf.set("path", args[1] + "/frequent/1");
		
		Configuration conf2 = HBaseConfiguration.create();
		Connection cc = ConnectionFactory.createConnection(conf2);
		try {
			Admin a = cc.getAdmin();
			TableName tableName = TableName.valueOf("compressed");
			if(!a.tableExists(tableName)) {
				HTableDescriptor table = new HTableDescriptor(tableName);
				HColumnDescriptor family = new HColumnDescriptor(Bytes.toBytes("f"));
				table.addFamily(family);
				a.createTable(table);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			cc.close();
		}
		
		Job job = Job.getInstance(conf, "Database compression");

		job.setJarByClass(App.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setMapperClass(MapCompression.class);
		job.setCombinerClass(Combine.class);
		//job.setReducerClass(ReduceCompression.class);
		
		job.setNumReduceTasks(Integer.parseInt(args[4]));
		
		job.setInputFormatClass(TextInputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
		
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//FileOutputFormat.setOutputPath(job, new Path(args[1] + "/compressed"));
		
		TableMapReduceUtil.initTableReducerJob("compressed", ReduceCompression.class, job);

		return job;	
	}
	
	/*	NOT USED
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
	 * Create a vertical database containing frequent 1-itemsets associated with the list of their transactions
	 * Requirements : <output-path>/frequent/1 && <output-path>/compressed
	 * Results output : <output-path>/final/1
	 */
	private static Job setupJobFirstIteration(Configuration conf, String[] args) throws Exception{		
        conf.set("path", args[1] + "/frequent/1");
        
        Configuration conf2 = HBaseConfiguration.create();
        Connection cc = ConnectionFactory.createConnection(conf2);
		try {
			Admin a = cc.getAdmin();
			TableName tableName = TableName.valueOf("final-1");
			if(!a.tableExists(tableName)) {
				HTableDescriptor table = new HTableDescriptor(tableName);
				HColumnDescriptor family = new HColumnDescriptor(Bytes.toBytes("f"));
				table.addFamily(family);
				a.createTable(table);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			cc.close();
		}
	    	    
	    Scan scan = new Scan();
	    scan.setBatch(1);
	    scan.setCaching(500);
	    scan.setCacheBlocks(false);
		
		Job job = Job.getInstance(conf2, "Mining 1-itemsets");
		
		job.setJarByClass(App.class);
		
		TableMapReduceUtil.initTableMapperJob("compressed", scan, MapFirstIter.class, Text.class, Text.class, job);
		
		TableMapReduceUtil.initTableReducerJob("final-1", ReduceFirstIter.class, job);
		
		//job.setMapOutputKeyClass(Text.class);
		//job.setMapOutputValueClass(Text.class);
		//job.setOutputKeyClass(Text.class);
		//job.setOutputValueClass(Text.class);
		
		//job.setMapperClass(MapFirstIter.class);
		job.setCombinerClass(CombineFirstIter.class);
		//job.setReducerClass(ReduceFirstIter.class);
		
		job.setNumReduceTasks(Integer.parseInt(args[4]));
		
		//job.setInputFormatClass(TextInputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
		
		//FileInputFormat.addInputPath(job, new Path(args[1]+"/compressed"));		
		//FileOutputFormat.setOutputPath(job, new Path(args[1]+"/final/1"));
		
		return job;
	}
	
	private static Job setupJobSecondIteration(Configuration conf, String[] args) throws Exception{				
		Job job = Job.getInstance(conf, "Mining 2-itemsets");
		
		job.setJarByClass(App.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(MapSecIter.class);
		job.setPartitionerClass(PartitionSecIter.class);
		job.setSortComparatorClass(CompareSecIter.class);
		job.setReducerClass(ReduceSecIter.class);
		
		job.setNumReduceTasks(Integer.parseInt(args[4]));
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[1]+"/final/1"));
		FileOutputFormat.setOutputPath(job, new Path(args[1]+"/finalNoSize/2"));
		
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
		
		FileInputFormat.addInputPath(job, new Path(args[1]+"/final/" + iteration + "/raw/concat"));
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
		conf.set("path", "sizes/part");
		
		Job job = Job.getInstance(conf, "Cleaning unfrequent k-itemsets");
		
		job.setJarByClass(App.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(MapFreqClean.class);
		job.setPartitionerClass(PartitionFreqClean.class);
		job.setSortComparatorClass(CompareFreqClean.class);
		job.setReducerClass(ReduceFreqClean.class);
		
		job.setNumReduceTasks(Integer.parseInt(args[4]));
		
		job.setInputFormatClass(TextInputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[1]+"/finalNoSize/" + iteration));
		FileInputFormat.addInputPath(job, new Path(args[1]+"/frequent/" + iteration));
		MultipleOutputs.addNamedOutput(job, "dir", TextOutputFormat.class, Text.class, Text.class);
		//MultipleOutputs.setCountersEnabled(job, true);
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/final/" + iteration));
		
		return job;
	}
	
	private static Job setupJobMerge(Configuration conf, String[] args, int iteration) throws Exception{
		conf.set("path", args[1] + "/final/" + iteration + "/raw/");
		
		Job job = Job.getInstance(conf, "Merging frequent itemsets files");
		
		job.setJarByClass(App.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(MapMerge.class);
		job.setCombinerClass(Combine.class);
		job.setReducerClass(ReduceMerge.class);
		
		job.setNumReduceTasks(Integer.parseInt(args[4]));
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[1]+"/final/" + iteration  + "/sizes/"));
		
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
		
		PrintStream out = new PrintStream(new FileOutputStream("/home/lifoadm/output.txt"));
		PrintStream outErr = new PrintStream(new FileOutputStream("/home/lifoadm/outputErr.txt"));
		System.setOut(out);
		System.setErr(outErr);
		
		long startTime = System.currentTimeMillis();
		int exitCode = ToolRunner.run(new Configuration(), new App(), args);
		long endTime = System.currentTimeMillis();		
		
		System.out.println("Total time : " + (endTime - startTime)/1000 + " seconds.");		
		System.exit(exitCode);

	}
		/*
		 * TESTING PURPOSE
		 */
		public static Job setupJobHBase(Configuration conf, String[] args) throws Exception {

	    Configuration config = HBaseConfiguration.create();
	    //config.set(TableInputFormatBase.MAPREDUCE_INPUT_AUTOBALANCE, "true");
	    //config.set(TableInputFormatBase.MAX_AVERAGE_REGION_SIZE, "67108864");
	    //config.set(TableInputFormatBase.NUM_MAPPERS_PER_REGION, "4");
	    	    
	    Scan scan = new Scan();
	    scan.setBatch(1);
	    scan.setCaching(500);
	    scan.setCacheBlocks(false);
	    
	    //config.set(TableInputFormat.SCAN, TableMapReduceUtil.convertScanToString(scan));
	    //config.set(TableInputFormat.INPUT_TABLE, "compressed");
	    
	    Job job = Job.getInstance(config, "Test HBase");
	    job.setJarByClass(App.class);

	    //job.setMapperClass(MapHBase.class);
	    //job.setInputFormatClass(TableInputFormat.class);
	    
	    /*List<Scan> scans = new ArrayList<Scan>();
	    for(int i = 0; i<Integer.parseInt(args[4]);i++) {
	    	Scan scan = new Scan();
	    	scan.setBatch(1);
	    	scan.setCaching(500);
		    scan.setCacheBlocks(false); 
		    scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("compressed"));
		    scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes(i+""));
		    scans.add(scan);
	    }*/
	    
	    

	    TableMapReduceUtil.initTableMapperJob("compressed", scan, MapHBase.class, null, null, job);
	    
	    //System.out.println(job.getConfiguration().getInt("hbase.mapreduce.tableinput.mappers.per.region", 0));
	    
	    //TableMapReduceUtil.initTableMapperJob(scans, MapHBase.class, null, null, job);
	    
	    
	    //config.set(TableInputFormat.SCAN, TableMapReduceUtil.convertScanToString(scan));
	    //config.set(TableInputFormat.INPUT_TABLE, tableName);
		//job.setOutputKeyClass(Text.class);
		//job.setOutputValueClass(IntWritable.class);
		
		//job.setMapperClass(MapHBase.class);
		//job.setCombinerClass(Combine.class);
		//job.setReducerClass(ReduceFrequentTokens.class);
		
		//job.setNumReduceTasks(Integer.parseInt(args[4]));
		
		//job.setInputFormatClass(TableInputFormat.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		
		//FileInputFormat.addInputPath(job, new Path(args[0]));
		//FileOutputFormat.setOutputPath(job, new Path(args[1]+"/frequent/1"));
		
		return job;
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		boolean verbose = true;
		
		Configuration conf2 = HBaseConfiguration.create();
		Connection cc = ConnectionFactory.createConnection(conf2);
		
		try {
			Admin a = cc.getAdmin();
			TableName tableName = TableName.valueOf("compressed");
			if(a.tableExists(tableName)) {
				a.disableTable(tableName);
				a.deleteTable(tableName);
			}
			tableName = TableName.valueOf("freq");
			if(a.tableExists(tableName)) {
				a.disableTable(tableName);
				a.deleteTable(tableName);
			}
			tableName = TableName.valueOf("final-1");
			if(a.tableExists(tableName)) {
				a.disableTable(tableName);
				a.deleteTable(tableName);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			cc.close();
		}
		

		//compressing database with only frequent items and transaction frequencies
		Job job = setupJobFrequentTokens(this.getConf(), args);	
		job.waitForCompletion(verbose);
				
		job = setupJobCompression(this.getConf(), args);
		job.waitForCompletion(verbose);
		
		//job = setupJobHBase(this.getConf(), args);
		//job.waitForCompletion(verbose);
	
		//creating vertical database
		job = setupJobFirstIteration(this.getConf(), args);
		job.waitForCompletion(verbose);
		
		//mining frequent 2-itemsets
		/*job = setupJobSecondIteration(this.getConf(), args);
		job.waitForCompletion(verbose);
		
		//mining frequent k-itemsets, from 3 to n
		int iter = 2;
		
		while(true){
			
			job = setupJobMiningFrequent(this.getConf(), args, iter);
			job.waitForCompletion(verbose);
			
			long records = job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_OUTPUT_RECORDS").getValue();
			
			if(records == 0) break;	//if there's no results, don't start another job
			
			job = setupJobCleaningFrequent(this.getConf(), args, iter);
			job.waitForCompletion(verbose);
			
			job = setupJobMerge(this.getConf(), args, iter);
			job.waitForCompletion(verbose);
			
			records = job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_OUTPUT_RECORDS").getValue();
			
			if(records == 0) break;	//if there's no results, don't start another job
			
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
		
			FileUtil.copyMerge(hdfs, new Path(args[1]+"/frequent/" + i), hdfs, new Path(args[1]+"/concat/" + i), false, this.getConf(), null);
			
		}
		
		
		
		//display
		displayFinalData(this.getConf(), args);*/
		
		return 0;
	}





}
