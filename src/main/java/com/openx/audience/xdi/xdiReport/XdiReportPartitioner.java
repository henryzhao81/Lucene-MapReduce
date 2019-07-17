package com.openx.audience.xdi.xdiReport;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class XdiReportPartitioner extends Partitioner<Text, Text> {

  @Override
  public int getPartition(Text key, Text value, int numPartitions) {
    Integer in = key.toString().hashCode();
    int id = (int)(Math.abs(in.longValue()) % numPartitions);
    return id;
  }
}
