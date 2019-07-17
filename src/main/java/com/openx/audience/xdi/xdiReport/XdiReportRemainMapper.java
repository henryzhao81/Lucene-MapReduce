package com.openx.audience.xdi.xdiReport;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class XdiReportRemainMapper extends Mapper<Object, Text, Text, Text> {
  private Text first = new Text();
  private Text second = new Text();

  @Override
  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {	
    String[] words = value.toString().split("\t");
    if(words.length > 1) {
      first.set(words[0].trim());
      //second.set(words[1].trim() + "&screen6");//[Henry], at reducer output remain as format (u_viewerId_x  u_viewerId_y&vendor)
      second.set(words[1].trim());
      //System.out.println("first : " + words[0] + " second : " + words[1]);
      context.write(first, second);
    }
  }
}
