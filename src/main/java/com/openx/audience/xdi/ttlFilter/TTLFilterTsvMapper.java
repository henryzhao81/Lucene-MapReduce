package com.openx.audience.xdi.ttlFilter;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TTLFilterTsvMapper extends Mapper<Object, Text, Text, Text> {
  private Text first = new Text();
  private Text second = new Text();

  @Override
  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    String[] words = value.toString().split("\t");
    if(words.length > 1) {
      String strKey = words[0].trim();
      StringBuffer strValue = new StringBuffer();
      for(int i = 1; i < words.length; i++) {
        strValue.append(words[i].trim());
        if(i != (words.length - 1)) {
          strValue.append("&");
        }
      }
      first.set(strKey);
      second.set(strValue.toString());
      context.write(first, second);
    }
  }
}
