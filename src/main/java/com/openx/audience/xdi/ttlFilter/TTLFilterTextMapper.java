package com.openx.audience.xdi.ttlFilter;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TTLFilterTextMapper extends Mapper<Object, Text, Text, Text> {
  private Text first = new Text();
  private Text second = new Text();

  @Override
  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    first.set(value.toString().trim());
    second.set("expired");
    context.write(first, second);
  }
}
