package com.openx.audience.xdi.xdiReport;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class XdiReportScreen6Mapper extends Mapper<Object, Text, Text, Text> {
  private Text first = new Text();
  private Text second = new Text();
  private Text first2 = new Text();
  private Text second2 = new Text();

  @Override
  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {	
    String[] words = value.toString().split("\u0001");
    if(words.length > 1) {
      String[] result = new String[2];
      for(int i = 0; i < 2; i++) {
        String word = words[i];
        if(word.contains("\u0002") && word.startsWith("\u0002")) {
          word = word.substring(word.indexOf("\u0002") + 1).trim();
        } else {
          word = word.trim();
        }
        result[i] = word;
      }
      //reverse key and value, <key>^A<value> => value key&screen6
      //same value will send twice, reverse and unreverse
      if(result[1] != null && result[1].length() > 0 && !result[1].equals("\\N")) {
        first.set(result[1]); //Value become Key
        second2.set(result[1] + "&screen6");
      }
      if(result[0] != null && result[0].length() > 0 && !result[0].equals("\\N")) {
        second.set(result[0] + "&screen6"); //Key become Value+Vendor
        first2.set(result[0]);
        context.write(first, second);
        context.write(first2, second2);
      }			
    }
  }
}
