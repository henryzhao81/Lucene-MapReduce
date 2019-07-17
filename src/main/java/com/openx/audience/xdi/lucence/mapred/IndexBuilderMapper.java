package com.openx.audience.xdi.lucence.mapred;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class IndexBuilderMapper extends Mapper<Object, Text, Text, Text> {
	private Text first = new Text();
	private Text second = new Text();
	
	private final static Map<String, Integer> weightMap;
	static {
		weightMap = new HashMap<String, Integer>();
		weightMap.put("Internet Explorer", 100);
		weightMap.put("Chrome", 80);
		weightMap.put("Firefox", 60);
		weightMap.put("Other", 40);
	}
	
    //5  ->  3
    //8  ->  4
    //15 ->  14 
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		//String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		//String[] words = value.toString().split("\u0001");
		String[] words = value.toString().split("\t");
		long timestamp = -1;	
		String vid = null;
		String mid = null;
		String browserType = null;
		if(words.length >= 1) {
			try {
				//timestamp = Long.parseLong(words[0]);
				//2015-09-23_00:00:44
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss");
				Date date = sdf.parse(words[0].trim());
				timestamp = date.getTime();

			}catch(Exception ex) {
				System.out.println("words[0] failed to convert to timestamp " + ex.getMessage());
			}
			if(words.length >= 3 && words[3] != null && words[3].length() > 0 && !words[3].equals("\\N"))
				vid = words[3].trim();
			if(words.length >= 4 && words[4] != null && words[4].length() > 0 && !words[4].equals("\\N"))
				mid = words[4].trim();
			if(words.length >= 14 && words[14] != null && words[14].length() > 0 && !words[14].equals("\\N")) {
				browserType = words[14].trim();
				if(!weightMap.containsKey(browserType))
					browserType = null;
			}
		}
		if(timestamp > 0 && vid != null && mid != null && browserType != null) {
			//System.out.println("timestamp : " +  timestamp + " vid : " +  vid + " mid : " +  mid + " browserType : " + browserType);
			String strFirst = vid;
			String strSecond = timestamp + "&" + mid + "&" + browserType;
			first.set(strFirst);
			second.set(strSecond);
			context.write(first, second);
		}
	}
}
