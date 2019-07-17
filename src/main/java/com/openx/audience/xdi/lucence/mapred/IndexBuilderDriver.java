package com.openx.audience.xdi.lucence.mapred;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class IndexBuilderDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new IndexBuilderDriver(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapred.input.dir.recursive", "true");
		if(args.length > 2) {
			String newData = args[2];
			conf.set("current_date", newData);
		}
		if(args.length > 3) {
			String preData = args[3];
			conf.set("previous_date", preData);
		}
	    Job job = Job.getInstance(conf, "index_builder");
	    job.setJarByClass(IndexBuilderDriver.class);
	    job.setMapperClass(IndexBuilderMapper.class);
	    job.setReducerClass(IndexBuilderReducer.class);
	    job.setNumReduceTasks(11);//change to prime number
	    job.setPartitionerClass(IndexBuilderPartitioner.class);
	    //job.setGroupingComparatorClass(IndexBuilderGroupComparator.class);
	    job.setInputFormatClass(SequenceFileInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    //FileInputFormat.setInputDirRecursive(job, true);//FIXME : not supported API
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    return job.waitForCompletion(true) ? 0 : 1;
	}
	
	private static final String hdfsURL = "hdfs://xvaic-sg-36.xv.dc.openx.org:8022";//PROD
	//private static final String hdfsURL = "hdfs://xvaa-i35.xv.dc.openx.org:8020";//QA
	private static final String xdiBasePath = "/user/dmp/xdi/xdi-market-request/y=2015/";
	private static final String xdiOutPath = "/user/dmp/xdi/output/index_";
	private List<String[]> listPaths(String start, String end) {
		List<String> paths = new ArrayList<String>();
		FileSystem remoteFileSystem = null;
		try {
			URI hdfsNamenode = new URI(hdfsURL);
			remoteFileSystem = FileSystem.get(hdfsNamenode, new Configuration());		
			Path source = new Path(xdiBasePath);
			final FileStatus[] years = remoteFileSystem.globStatus(source);
			for(FileStatus eachYear : years) {
				if(eachYear.isDirectory()) {
					final FileStatus[] allMonth = remoteFileSystem.listStatus(eachYear.getPath());
					for(FileStatus eachMonth : allMonth) { ///user/dmp/xdi/xdi-market-request/y=2015/m=x
						if(eachMonth.isDirectory()) {
							final FileStatus[] allDays = remoteFileSystem.listStatus(eachMonth.getPath());
							for(FileStatus eachDay : allDays) { ///user/dmp/xdi/xdi-market-request/y=2015/m=x/d=x
								if(eachDay.isDirectory()) {
									final FileStatus[] allHours = remoteFileSystem.listStatus(eachDay.getPath());
									for(FileStatus eachHour : allHours) {
										if(eachHour.isDirectory()) {
											String eachHourPath = eachHour.getPath().toString();
											String subPath = eachHourPath.substring(eachHourPath.indexOf(xdiBasePath) + xdiBasePath.length());
											paths.add(subPath);
										}
									}
								}
							}
						}
					}
				}
			}
			Collections.sort(paths, new Comparator<String>() {
				@Override
				public int compare(String str1, String str2) {
					String[] s_str1 = str1.split("/");
					String c_str1 = "";
					for(String sub : s_str1) {
						c_str1 += sub.substring(sub.indexOf("=") + 1);
					}
					String[] s_str2 = str2.split("/");
					String c_str2 = "";
					for(String sub : s_str2) {
						c_str2 += sub.substring(sub.indexOf("=") + 1);
					}
					return  Integer.parseInt(c_str1) - Integer.parseInt(c_str2);
				}
			});
			String previous = start;
			List<String[]> result = new ArrayList<String[]>();
			for(String eachPath : paths) {
				String[] array = new String[4];
				if(this.convertToInteger(eachPath) > this.convertToInteger(start) && this.convertToInteger(eachPath) <= this.convertToInteger(end)) {
					array[0] = xdiBasePath + eachPath;
					array[1] = xdiOutPath + this.convertToInteger(eachPath);
					array[2] = ""+this.convertToInteger(eachPath);
					array[3] = ""+this.convertToInteger(previous);
					System.out.println("------> add parameter inputPath : " + array[0] + " outputPath : " + array[1] + " current : " + array[2] + " previous : " + array[3]);
					result.add(array);
					previous = eachPath;
				}
			}
			return result;
		}catch(Exception ex) {
			ex.printStackTrace();
		}finally{
			try {
			if(remoteFileSystem != null)
				remoteFileSystem.close();
			}catch(Exception e) {
				e.printStackTrace();
			}
		}
		return null;
	}
	
	private int convertToInteger(String path) {
		String[] s_str = path.split("/");
		String c_str = "";
		for(String sub : s_str) {
			c_str += sub.substring(sub.indexOf("=") + 1);
		}
		return Integer.parseInt(c_str);
	}

	/*
	@Override
	public int run(String[] args) throws Exception {
//		Configuration conf = new Configuration();
		List<String[]> results = this.listPaths(args[0], args[1]);//"m=08/d=28/h=14"
		if(results != null && results.size() > 0) {
			for(String[] result : results) {
				Configuration conf = new Configuration();
				String newInputPath = result[0];
				String newOutputPath = result[1];
				String current = result[2];
				conf.set("current_date", current);
				String previous = result[3];
				conf.set("previous_date", previous);
				System.out.println(" =======> inputPath : " + newInputPath + " outputPath : " + newOutputPath + " current : " + current + " previous : " + previous);
				Job job = Job.getInstance(conf, ("index_builder_"+current));
				job.setJarByClass(IndexBuilderDriver.class);
				job.setMapperClass(IndexBuilderMapper.class);
				job.setReducerClass(IndexBuilderReducer.class);
				job.setNumReduceTasks(10);
				job.setPartitionerClass(IndexBuilderPartitioner.class);
				//job.setGroupingComparatorClass(IndexBuilderGroupComparator.class);
				job.setInputFormatClass(SequenceFileInputFormat.class);
				job.setOutputFormatClass(TextOutputFormat.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				FileInputFormat.setInputDirRecursive(job, true);
				FileInputFormat.addInputPath(job, new Path(newInputPath));
				FileOutputFormat.setOutputPath(job, new Path(newOutputPath));
				//return job.waitForCompletion(true) ? 0 : 1;
				job.waitForCompletion(true);
				try {
					System.out.println("Job finished, wait for 10s");
					Thread.currentThread().sleep(10000);
				}catch(Exception ex) {
					ex.printStackTrace();
				}
			}
		}
		return 0;
	}
	*/
}
