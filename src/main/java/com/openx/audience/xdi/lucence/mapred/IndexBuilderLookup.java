package com.openx.audience.xdi.lucence.mapred;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class IndexBuilderLookup {
	private final static Map<String, Integer> weightMap;
	static {
		weightMap = new HashMap<String, Integer>();
		weightMap.put("Internet Explorer", 100);
		weightMap.put("Chrome", 80);
		weightMap.put("Firefox", 60);
		weightMap.put("Other", 40);
	}
	
	public static void main(String[] args) throws Exception{
		IndexBuilderLookup lookup = new IndexBuilderLookup();
		//int res = lookup.run(args);
		//lookup.listTarget("20151022", 4);
		//lookup.readFile();
        String input = "/user/dmp/xdi/xdi-market-request/y=2015/m=10/d=06/h=00";
        String token = "/user/dmp/xdi/xdi-market-request/";
        String output = input.substring(input.indexOf(token) + token.length());
        String[] split = output.split("/");
        //[y=2015, m=10, d=06, h=00]
        String result = "";
        for(String each : split) {
        	System.out.println("unit : " + each.substring(0, each.indexOf("=")));
        	result += each.substring(each.indexOf("=") + 1) + "/";
        }
        System.out.println(output + " | " + split.length + " | " + (System.currentTimeMillis()));
        lookup.sortName();
        lookup.readLine("/Users/henry.zhao/Work/index/part-r-00000");
		System.exit(0);
	}

	public int run(String[] args) throws Exception {
		List<String[]> results = this.listPaths(args[0], args[1]);//"m=08/d=28/h=14"
		if(results != null && results.size() > 0) {
			for(String[] result : results) {
				String newInputPath = result[0];
				String newOutputPath = result[1];
				String current = result[2];
				String previous = result[3];
				System.out.println(" =======> inputPath : " + newInputPath + " outputPath : " + newOutputPath + " current : " + current + " previous : " + previous);
				
			}
			return 0;
		}
		return 1;
	}
	
	private void sortName() {
		List<Path> targets = new ArrayList<Path>();
		Path p0 = new Path("/var/tmp/source/2015/10/05/00/xdi-20151005004700-1444006020-task_201508140106_106778_m_000119.seq");
		Path p1 = new Path("/var/tmp/source/2015/10/05/00/xdi-20151005000500-1444003500-task_201508140106_106682_m_000509.seq");
		Path p2 = new Path("/var/tmp/source/2015/10/05/00/xdi-20151005002500-1444004700-task_201508140106_106737_m_000434.seq");
		Path p3 = new Path("/var/tmp/source/2015/10/05/00/xdi-20151005002700-1444004820-task_201508140106_106737_m_000578.seq");
		Path p4 = new Path("/var/tmp/source/2015/10/05/00/xdi-20151005000700-1444003620-task_201508140106_106682_m_000323.seq");
		Path p5 = new Path("/var/tmp/source/2015/10/05/00/xdi-20151005004800-1444006080-task_201508140106_106778_m_000059.seq");
		Path p6 = new Path("/var/tmp/source/2015/10/05/00/xdi-20151005000500-1444003500-task_201508140106_106682_m_000014.seq");
		Path p7 = new Path("/var/tmp/source/2015/10/05/00/xdi-20151005004500-1444005900-task_201508140106_106778_m_000611.seq");
		Path p8 = new Path("/var/tmp/source/2015/10/05/00/xdi-20151005002700-1444004820-task_201508140106_106737_m_000075.seq");
		Path p9 = new Path("/var/tmp/source/2015/10/05/00/xdi-20151005000700-1444003620-task_201508140106_106682_m_001061.seq");
		targets.add(p0);
		targets.add(p1);
		targets.add(p2);
		targets.add(p3);
		targets.add(p4);
		targets.add(p5);
		targets.add(p6);
		targets.add(p7);
		targets.add(p8);
		targets.add(p9);
		Collections.sort(targets, new Comparator<Path>() {
			@Override
			public int compare(Path p1, Path p2) {
				String str1 = p1.getName();
				String str2 = p2.getName();
				String[] c_str1 = str1.split("-");
				String[] c_str2 = str2.split("-");
				return (int)(Long.parseLong(c_str1[1]) - Long.parseLong(c_str2[1]));
			}
		});
		for(Path p : targets) {
			System.out.println(p.toString());
		}
		System.out.println("=============");
	}
	
	public void readLine(String filePath) {
		try {
			InputStream fis = new FileInputStream(filePath);
			InputStreamReader isr = new InputStreamReader(fis);
			BufferedReader br = new BufferedReader(isr);
			String line;
			while((line = br.readLine()) != null) {
				String[] pair = line.split("\t");
				String key = pair[0].trim();
				String value = pair[1].trim();
				System.out.println(key + " - " + value  + " partition : " + this.getPartition(key, null, 11));
			}
		} catch(IOException ex) {
			ex.printStackTrace();
		}
	}
	
    public int getPartition(String key, String value, int numPartitions) {
		Integer in = key.hashCode();
		int id = (int)(Math.abs(in.longValue()) % numPartitions);
		return id;
	}
	
	private void readFile() throws Exception {
		String fileName = "/Users/henry.zhao/Documents/workspace/xdi-20151005005900-1444006740-task_201508140106_106778_m_000126.seq";//this.getLocalPath();
		System.out.println("read from local path : " + fileName);
		Configuration conf = new Configuration();
		Path path = new Path(fileName);
		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
			Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
			while(reader.next(key, value)) {
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
					System.out.println("timestamp : " +  timestamp + " vid : " +  vid + " mid : " +  mid + " browserType : " + browserType);
				}
			}
		}catch(Exception ex) {

		}finally{
			IOUtils.closeStream(reader);
		}
	}
	
	public void listTarget(String sourceIndex, int limits) throws Exception {
		String inputPath = "/user/dmp/xdi/result/20150926/screen6/diff*.gz";
		String[] eachs = inputPath.split("/");
		    System.out.println(eachs[5]);
		
		int[] dates = new int[] {20150928, 20150916, 20150904, 20150910, 20151022, 20151016, 20150922, 20151004, 20151010};
		Arrays.sort(dates);
		int i = 0;
		Queue<Integer> queue = new LinkedList<Integer>();
		for(int date : dates) {
			queue.offer(date);
			if(i >= limits)
				queue.poll();
			if(Integer.parseInt(sourceIndex) <= date) {
		        break;
			}
		    i++;
		}
		if(i >= dates.length || Integer.parseInt(sourceIndex) > dates[i])
			throw new Exception("Index for current date have not been generated yet");
		List<String> res = new ArrayList<String>();
		while(!queue.isEmpty()) {
			res.add(Integer.toString(queue.poll()));
		}
		Collections.sort(res, Collections.reverseOrder());
		for(String e : res) {
			System.out.println(e);
		}
		System.out.println("=====");
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
}
