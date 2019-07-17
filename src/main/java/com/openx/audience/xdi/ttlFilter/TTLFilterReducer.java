package com.openx.audience.xdi.ttlFilter;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TTLFilterReducer extends Reducer<Text, Text, Text, Text> {
  private Text result = new Text();
  /*
	private MultipleOutputs<Text, Text> mos;
    private String resultOutput;
    private String deleteOutput;
   */

  @Override
  protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException, InterruptedException {
    int reduceTaskId = context.getTaskAttemptID().getTaskID().getId();
    System.out.println("======== setup at task : " + reduceTaskId);
    //mos = new MultipleOutputs(context);
    //resultOutput = context.getConfiguration().get("result_output");
    //deleteOutput = context.getConfiguration().get("delete_output");
    //System.out.println("========> resultOutput : " + reduceTaskId + "=============> deleteOutput : " + deleteOutput);
  }

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    boolean expired = false;
    int index = 0;
    String[] current = new String[3];
    for(Text value : values) {
      if((value.toString()).equals("expired")) {
        expired = true;
        break;
      } else {
        String[] input = value.toString().split("&");//[0] market_id, [1] browser_type, [2] vendor
        if(input.length > 2) {
          current[0] = input[0].trim();
          current[1] = input[1].trim();
          current[2] = input[2].trim();
        }
      }
      index++;
    }
    /*
		if(expired) {
			if(index > 1) {
				result.set(Integer.toString(index));
				mos.write("delete", key, result, deleteOutput);
			}
		} else {
			StringBuffer buffer = new StringBuffer();
			buffer.append(current[0]);
			buffer.append("\t");
			buffer.append(current[1]);
			buffer.append("\t");
			buffer.append(current[2]);
			result.set(buffer.toString());
			mos.write("result", key, result, resultOutput);
		}
     */
    if(!expired) {
      StringBuffer buffer = new StringBuffer();
      buffer.append(current[0]);
      buffer.append("\t");
      buffer.append(current[1]);
      buffer.append("\t");
      buffer.append(current[2]);
      result.set(buffer.toString());
      context.write(key, result);
    }
  }
}
