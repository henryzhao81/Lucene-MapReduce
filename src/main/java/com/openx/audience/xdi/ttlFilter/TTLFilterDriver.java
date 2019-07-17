package com.openx.audience.xdi.ttlFilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.openx.audience.xdi.xdiFilter.TsvOutputFormat;

public class TTLFilterDriver extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new TTLFilterDriver(), args);
    System.exit(res);
  }

  /**
   * args:  
   * [0] : /user/dmp/xdi/result/20151010/screen6/ttl*.gz  or more split by ','
   * [1] : /user/dmp/xdi/output/tsv/20151026/part*.tsv
   * [2] : /user/dmp/xdi/output/tsv/filter/20151010
   */
  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = new Configuration();
    if(args.length < 3)
      throw new Exception("3 parameter required");
    //		String resultOutput = args[2];
    //		conf.set("result_output", resultOutput);
    //		System.out.println("result output : " + resultOutput);
    //		String deleteOutput = args[3];
    //		conf.set("delete_output", deleteOutput);
    //		System.out.println("delete output : "  + deleteOutput);
    String[] multiTtlInputs = args[0].split(",");
    if(multiTtlInputs == null || multiTtlInputs.length == 0)
      throw new Exception("no input parameter");
    String tsvInputPath = args[1];
    Job job = Job.getInstance(conf, "TtlFilter");
    job.setJarByClass(TTLFilterDriver.class);
    job.setReducerClass(TTLFilterReducer.class);
    job.setPartitionerClass(TTLFilterPartitioner.class);
    job.setNumReduceTasks(100);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    if(tsvInputPath != null && tsvInputPath.length() > 0) {
      System.out.println("tsvInputPath : " + tsvInputPath);
      MultipleInputs.addInputPath(job, new Path(tsvInputPath), TextInputFormat.class, TTLFilterTsvMapper.class);
    }
    int index = 0;
    for(String ttlInput : multiTtlInputs) {
      System.out.println("ttlInput["+index+"] : " + ttlInput);
      MultipleInputs.addInputPath(job, new Path(ttlInput), TextInputFormat.class, TTLFilterTextMapper.class);
      index++;
    }
    job.setOutputFormatClass(TsvOutputFormat.class);
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    //		MultipleOutputs.addNamedOutput(job, "result", TsvOutputFormat.class, Text.class, Text.class);
    //		MultipleOutputs.addNamedOutput(job, "delete", TextOutputFormat.class, Text.class, Text.class);
    return job.waitForCompletion(true) ? 0 : 1;
  }

}
