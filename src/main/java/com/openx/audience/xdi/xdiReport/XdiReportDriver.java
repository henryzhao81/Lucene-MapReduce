package com.openx.audience.xdi.xdiReport;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.openx.audience.xdi.clean.TempFolderCleanDriver;
import com.openx.audience.xdi.clean.TempFolderCleanMapper;
import com.openx.audience.xdi.clean.TempFolderCleanPartitioner;
import com.openx.audience.xdi.clean.TempFolderCleanReducer;

public class XdiReportDriver extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new XdiReportDriver(), args);
    System.exit(res);
  }

  /**
   * args[0] : inputPath
   * args[1] : outputPath
   */
  /*
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
//		String inputPath = args[0];
//		String[] eachs = inputPath.split("/");
//		if(eachs != null && eachs.length >= 5) {
//			System.out.println("look for index of date : " + eachs[5]);
//			conf.set("target_index", eachs[5]);
//		} else {
//			System.out.println("Wrong input path");
//			throw new Exception("wrong input path");
//		}
		if(args.length < 5) {
			throw new Exception("insufficent parameter !");
		}
		conf.set("result_output", args[1]);
		conf.set("remain_output", args[2]);
		conf.set("target_index", args[3]);
		boolean parseRemain = Boolean.parseBoolean(args[4]);
		System.out.println("inputPath : " + args[0] + " result_output : " + args[1] + " remain_output : " + args[2] + " target_index : " + args[3] + " parseRemain : " + parseRemain);
		Job job = Job.getInstance(conf, "xdiReport");
		job.setJarByClass(XdiReportDriver.class);
		if(parseRemain)
			job.setMapperClass(XdiReportRemainMapper.class);
		else
		    job.setMapperClass(XdiReportScreen6Mapper.class);
		job.setReducerClass(XdiReportReducer.class);
		job.setNumReduceTasks(11);//WARN: (IMPORTANT) Must be same as index partition
		job.setPartitionerClass(XdiReportPartitioner.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputDirRecursive(job, true);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		MultipleOutputs.addNamedOutput(job, "result", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "remain", TextOutputFormat.class, Text.class, Text.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}
   */
  
  @Override
  public int run(String[] args) throws Exception {
    String[] strStat = new String[2];
    String inputXdi = null;
    if(args != null && args.length > 0) {
      inputXdi = args[0];
    } else {
      inputXdi = this.start(strStat);
    }
    System.out.println("input xdi result is : " + inputXdi);
    if(args != null && args.length > 1) {
      outputPath = args[1];
      System.out.println("reset output path to " + outputPath);     
    }
    if(inputXdi == null || inputXdi.length() == 0)
      throw new Exception("Cannot find correct input xdi file !");
    int[] part = new int[1];
    List<String[]> paraList = this.getParameterList(inputXdi, part, strStat[1]);
    boolean jobSuccess = false;
    if(paraList != null && paraList.size() > 0) {
      int size = paraList.size();
      int jobSeq = 1;
      for(String[] paras : paraList) {
        try {
          jobSuccess = this.executeXdiReport(paras);
        }catch(Exception ex) {
          System.out.println("Exception during execute xdireport job - " + ex.getMessage());
          ex.printStackTrace();
        }
        System.out.println("XdiReport Job "+jobSeq+"/"+size+" finished, wait for 10s");
        Thread.currentThread().sleep(10000);
        try{
          System.out.println("execute clean up 1");
          this.executeCleanUp();
        }catch(Exception ex) {
          ex.printStackTrace();
        }
        try{
          System.out.println("execute clean up 2");
          this.executeCleanUp();
        }catch(Exception ex) {
          ex.printStackTrace();
        }
        System.out.println("Clean Job finished, wait for 10s");
        Thread.currentThread().sleep(10000);
        if(!jobSuccess) {
          strStat = null;
          System.out.println("XdiReport Job "+jobSeq+"/"+size+" Failed or Killed, skip rest of jobs , and clean up status file path to avoid update");
          this.removeOutputs(paras);
          System.out.println("Finish remove both batch and remain folder");
          break;
        }
        jobSeq++;
//        this.executeXdiReport(paras);
//        try {
//          System.out.println("XdiReport Job finished, wait for 10s");
//          Thread.currentThread().sleep(10000);
//        }catch(Exception ex) {
//          ex.printStackTrace();
//        }
//        this.executeCleanUp();
//        this.executeCleanUp();
//        try {
//          System.out.println("Clean Job finished, wait for 10s");
//          Thread.currentThread().sleep(10000);
//        }catch(Exception ex) {
//          ex.printStackTrace();
//        }
      }
    }
    this.end(strStat);
    return 0;
  }
  
  /**
   * remove output folder if job failed or killed, otherwise even retry, job will still be failed
   * @param paras
   */
  private void removeOutputs(String[] paras) {
    String batch = paras[1].substring(0, paras[1].indexOf("batch"));
    String remain = paras[2].substring(0, paras[2].indexOf("remain"));
    try {
      FileSystem remoteFileSystem = FileSystem.get(new URI(hdfsURL), new Configuration());
      System.out.println("delete batch path : " + batch);
      Path p_batch = new Path(batch);
      if(remoteFileSystem.exists(p_batch)) {
        remoteFileSystem.delete(p_batch, true);
      } else {
        System.out.println("batch path : " + batch + " not exist");
      }
      System.out.println("delete remain path : " + remain);
      Path p_remain = new Path(remain);
      if(remoteFileSystem.exists(p_remain)) { 
        remoteFileSystem.delete(p_remain, true);
      } else {
        System.out.println("remain path : " + remain + " not exist");
      }
    }catch(Exception ex) {
      ex.printStackTrace();
    }
  }

  /**
   * Run job generate xdi report
   * @param paras
   * @return
   * @throws Exception
   */
  public boolean executeXdiReport(String[] paras) throws Exception {
    Configuration conf = new Configuration();
    conf.set("result_output", paras[1]);
    conf.set("remain_output", paras[2]);
    conf.set("target_index", paras[3]);
    conf.set("current_date", paras[6]);
    conf.set("mapred.input.dir.recursive", "true");
    boolean parseRemain = Boolean.parseBoolean(paras[4]);
    System.out.println("inputPath : " + paras[0] + " result_output : " + paras[1] + " remain_output : " + paras[2] + " target_index : " + paras[3] + " parseRemain : " + parseRemain + " current date : " + paras[6]);
    Job job = Job.getInstance(conf, "xdiReport");
    job.setJarByClass(XdiReportDriver.class);
    if(parseRemain)
      job.setMapperClass(XdiReportRemainMapper.class);
    /* Set Mapper in MultipleInputs
    else
      job.setMapperClass(XdiReportScreen6Mapper.class);
    */
    job.setReducerClass(XdiReportReducer.class);
    //job.setNumReduceTasks(11);//WARN: (IMPORTANT) Must be same as index partition
    if(paras.length > 5) {
      int partitionSize = Integer.parseInt(paras[5]);
      System.out.println("partition size : " + partitionSize);
      job.setNumReduceTasks(partitionSize);
    } else {
      throw new Exception("missing partitions");
    }
    job.setPartitionerClass(XdiReportPartitioner.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    //FileInputFormat.setInputDirRecursive(job, true);//FIXME : not supported API
    //FileInputFormat.addInputPath(job, new Path(paras[0]));//TODO : Change to getMultiInputPath
    this.getMultiInputPath(paras[0], job, parseRemain);//[Henry] set multiple inputs
    
    FileOutputFormat.setOutputPath(job, new Path(paras[1]));
    MultipleOutputs.addNamedOutput(job, "result", TextOutputFormat.class, Text.class, Text.class);
    MultipleOutputs.addNamedOutput(job, "remain", TextOutputFormat.class, Text.class, Text.class);
    boolean success = job.waitForCompletion(true);
    //System.out.println("xdi report job status : " + job.getStatus().getState());
    //if(job.getStatus().getState() == JobStatus.State.SUCCEEDED)
    System.out.println("xdi report job success : " + success);
    if(success)
      return true;
    else
      return false;
  }
  
  //TODO, auto construct class
  private static Map<String, Class<? extends Mapper<Object, Text, Text, Text>>> mapping = new HashMap<String, Class<? extends Mapper<Object, Text, Text, Text>>>();
  static{
    mapping.put("screen6", XdiReportScreen6Mapper.class);//"com.openx.audience.xdi.xdiReport.XdiReportScreen6Mapper"
    //mapping.put("ubermedia", XdiReportUbermediaMapper.class);//"com.openx.audience.xdi.xdiReport.XdiReportUbermediaMapper"
  }
  
  //TODO, find sub vendor path
  public void getMultiInputPath(String inputs, Job job, boolean parseRemain) throws Exception {
    if(parseRemain) {
      FileInputFormat.addInputPath(job, new Path(inputs));
    } else {
      FileSystem remoteFileSystem = null;
      try {
        URI hdfsNamenode = new URI(hdfsURL);
        remoteFileSystem = FileSystem.get(hdfsNamenode, new Configuration());
        Path vendor = new Path(inputs, "*");
        final FileStatus[] vendors = remoteFileSystem.globStatus(vendor);
        if(vendors.length == 1) {
          Path vendorPath = vendors[0].getPath();
          System.out.println("Find Single Vendor : " + vendorPath.getName() + " --- " + vendorPath.toString());
          //job.setMapperClass(XdiReportScreen6Mapper.class);
          job.setMapperClass(mapping.get(vendorPath.getName()));
          FileInputFormat.addInputPath(job, vendorPath);
        } else {
          for(FileStatus ven : vendors) {
            if(ven.isDirectory()) {
              Path vendorPath = ven.getPath();
              System.out.println("Find Multi Vendor : " + vendorPath.getName() + " --- " + vendorPath.toString());
              Class clz = mapping.get(vendorPath.getName());
              if(clz != null) {
                System.out.println("Find Multi Vendor Mapper Class : " + clz.getName());
                MultipleInputs.addInputPath(job, vendorPath, TextInputFormat.class, clz);
              }
            }
          }
        }
      }catch(Exception ex) {
        ex.printStackTrace();
      }
    }
  }

  /**
   * clean up job
   * @throws Exception
   */
  public void executeCleanUp() throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.input.dir.recursive", "true");
    Job job = Job.getInstance(conf, "cleaner");
    job.setJarByClass(TempFolderCleanDriver.class);
    job.setMapperClass(TempFolderCleanMapper.class);
    job.setReducerClass(TempFolderCleanReducer.class);
    job.setNumReduceTasks(30);
    job.setPartitionerClass(TempFolderCleanPartitioner.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    //FileInputFormat.setInputDirRecursive(job, true);//FIXME : not supported API
    FileInputFormat.addInputPath(job, new Path("/user/dmp/xdi/output/xdi/result_*0"));
    FileOutputFormat.setOutputPath(job, new Path("/user/dmp/xdi/output/tmp/temp_" + (System.currentTimeMillis() / 1000)));
    boolean success = job.waitForCompletion(true);
    System.out.println("clean up job success : " + success);
  }

  /**
   * @param xdiFolder : /user/dmp/xdi/result/20151027/screen6/diff_*.gz
   * @return
   */
  private final static int indexMaxSize = 6; //right now each folder contains 3 days of index, search for 20 days, change from 6 to 7
  private static final String hdfsURL = "hdfs://xvaic-sg-36.xv.dc.openx.org:8022";//PROD
  //private static final String hdfsURL = "hdfs://xvaa-i35.xv.dc.openx.org:8020";//QA
  private static final String xdiInputPath = "/user/dmp/xdi/result/";
  //private static final String partIndexPath = "/user/dmp/xdi/index/part_0";
  private static final String partIndexPath = "/user/dmp/xdi/index/archive";
  private static String outputPath = "/user/dmp/xdi/output/xdi";
  public List<String[]> getParameterList(String inputXdi, int[] partition, String currentDate) {
    List<String[]> result = null;
    FileSystem remoteFileSystem = null;
    try {
      URI hdfsNamenode = new URI(hdfsURL);
      remoteFileSystem = FileSystem.get(hdfsNamenode, new Configuration());
      Path pPath = new Path(partIndexPath, "*");
      List<Integer> dates = this.checkTimeSpan(remoteFileSystem, pPath, inputXdi);
      String targetFolder = this.extractTargetFolder(inputXdi); //20151027
      if(Integer.parseInt(targetFolder) > dates.get(0))
        throw new Exception("Target folder : " + targetFolder + " bigger than existing latest index " + dates.get(0));
      String[] parameters = null;
      result = new ArrayList<String[]>();
      for(int i = 0; i < dates.size() && i < indexMaxSize; i++) {
        parameters = new String[7];
        parameters[1] = outputPath + "/xdiresult_" + targetFolder + "/batch_" + i;
        parameters[2] = outputPath + "/temp/xdiresult_" + targetFolder + "/remain_" + i;
        parameters[3] = Integer.toString(dates.get(i));
        if(i == 0) {
          parameters[0] = inputXdi;
          parameters[4] = "false";
          parameters[6] = currentDate;
        } else {
          parameters[0] = outputPath + "/temp/xdiresult_" + targetFolder + "/remain_" + (i-1) + "*";
          parameters[4] = "true";
          parameters[6] = parameters[3];
        }
        parameters[5] = this.getPartitionSize(remoteFileSystem, dates.get(i), parameters[6]);
        System.out.println("parameters : " + parameters[0] + " " + parameters[1] + " " + parameters[2] + " " + parameters[3] + " " + parameters[4] + " " + parameters[5] + " " + parameters[6]);
        result.add(parameters);
      }
    }catch(Exception ex) {
      System.out.println("Failed to generate command list " + ex.getMessage());
      ex.printStackTrace();
    }
    return result;
  }
  
  private static final int timeSpan = 30; //Days
  private List<Integer> checkTimeSpan(FileSystem remoteFileSystem, Path iPart, String inputXdi) throws IOException, ParseException {
    Path inputPath = new Path(inputXdi);
    int start = Integer.parseInt(inputPath.getName());
    final FileStatus[] indices = remoteFileSystem.globStatus(iPart);
    List<Integer> dates = new ArrayList<Integer>();
    for(FileStatus ind : indices) {
      if(ind.isDirectory() && indexExist(remoteFileSystem, ind.getPath().toString())) {
        dates.add(Integer.parseInt(ind.getPath().getName()));
      }
    }
    Collections.sort(dates, Collections.reverseOrder());
    List<Integer> res = new ArrayList<Integer>();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
    Calendar c = Calendar.getInstance();
    int index = 0;
    for(Integer each : dates) {
      c.setTime(sdf.parse(Integer.toString(each)));
      c.add(Calendar.DATE, timeSpan);
      int target = Integer.parseInt(sdf.format(c.getTime()));
      if(target > start && checkNext(index, dates, start))
        res.add(each);
      index++;
    }
    return res;
  }
  
  boolean checkNext(int index, List<Integer> dates, int start) {
    if(index < dates.size() - 1) {
      int next = dates.get(index + 1);
      if(next >= start)
        return false;
    }
    return true;
  }
  
  private boolean indexExist(FileSystem remoteFileSystem, String path) throws IOException{
    final FileStatus[] status = remoteFileSystem.globStatus(new Path(path, "*"));
    if(status != null && status.length > 0)
      return true;
    return false;
  }
  
  private String getPartitionSize(FileSystem remoteFileSystem, int tIndex, String currentDate) throws IllegalArgumentException, IOException {
//    Path inputPath = new Path(inputXdi);
//    int start = Integer.parseInt(inputPath.getName());
    System.out.println("Caculate partition size for path : " + (partIndexPath + "/" + tIndex + "/" + currentDate));
    final FileStatus[] parts = remoteFileSystem.globStatus(new Path(partIndexPath + "/" + tIndex + "/" + currentDate, "*"));
    return Integer.toString(parts.length);
  }

  private String extractTargetFolder(String inputXdi) {
    String rest = inputXdi.substring(inputXdi.indexOf(xdiInputPath) + xdiInputPath.length());
    return rest.substring(0, rest.indexOf("/"));
  }
  
  /**
   * First, check /user/dmp/xdi/result/, to get latest date of xdi to process, xdi_date
   * Second, check /user/dmp/xdi/index/status/DONE_XDI_REPORT_<DATE>, get get latest finish xdiReport job date. report_date
   * Compare those 2 date, xdi_date > report_date, then start job, other wise, throw exception
   * strStat[0] : old status file need delete, strStat[1] : new status file need create
   * @return, input parameter, command sample: HADOOP_USER_NAME=dmp hadoop jar xdiReport.jar /user/dmp/xdi/result/<RETURN>/screen6/diff_*.gz
   */
  private static final String statusPath = "/user/dmp/xdi/index/status/";
  private static final String resultPath = "/user/dmp/xdi/result/";
  private static final String STATUS_XDI_KEY = "DONE_XDI_REPORT_";
  public String start(String[] strStat) {
    FileSystem remoteFileSystem = null;
    try {
      URI hdfsNamenode = new URI(hdfsURL);
      remoteFileSystem = FileSystem.get(hdfsNamenode, new Configuration());
      Path rPath = new Path(resultPath, "*");
      final FileStatus[] dates = remoteFileSystem.globStatus(rPath);
      List<Integer> dateList = new ArrayList<Integer>();
      for(FileStatus date : dates) {
        if(date.isDirectory()) {
          dateList.add(Integer.parseInt(date.getPath().getName()));
        }
      }
      if(dateList == null || dateList.size() == 0)
        throw new Exception("cannot find any xdi result");
      Collections.sort(dateList, Collections.reverseOrder());
      int latest = dateList.get(0);
      strStat[1] = Integer.toString(latest);
      System.out.println("Latest date of xdi result is " + latest);
      String completeDate = null;
      Path status = new Path(statusPath, "*");
      final FileStatus[] statusFiles = remoteFileSystem.globStatus(status);
      int currentIndexFinish = -1;
      if(statusFiles != null && statusFiles.length > 0) {
        for(FileStatus stat : statusFiles) {
          String statName = stat.getPath().getName();
          if(statName.startsWith(STATUS_XDI_KEY)) {
            if(completeDate == null) {
              completeDate = statName.substring(statName.indexOf(STATUS_XDI_KEY) + STATUS_XDI_KEY.length());
              strStat[0] = completeDate;
              System.out.println("last complete xdireport job date is " + completeDate);
            } else {
              throw new Exception("Multiple " + STATUS_XDI_KEY + " found, please check if data correctly uploaded");
            }
          } else if(statName.startsWith(STATUS_KEY)) {
            String strCurrentIndexFinish = statName.substring(statName.indexOf(STATUS_KEY) + STATUS_KEY.length());
            try {
              currentIndexFinish = Integer.parseInt(strCurrentIndexFinish);
            }catch(Exception ex) {
              System.out.println("ERROR : failed to convert index finish time "  + strCurrentIndexFinish + " to integer");
            }
            System.out.println("current finished index from status file is : " + currentIndexFinish);
          }
        }
      }
      if(completeDate == null || completeDate.length() == 0)
        throw new Exception("cannot find latest xdireport job complete date");
      System.out.println("lastest third-party index result : " + latest + " last xdi result generated date : " + completeDate);
      if(latest > Integer.parseInt(completeDate) && indexReady(latest, currentIndexFinish, remoteFileSystem) && resultDone(remoteFileSystem, latest)) {
        //String update = resultPath + latest + "/screen6/diff_*.gz";//TODO, make /screen6/diff_*.gz configurable //[Henry] remove <vendor>/diff_*.gz
        String update = resultPath + latest + "/";
        System.out.println("ready to start xdi report job with input : " + update);
        return update;
      } else {
        throw new Exception("cannot start xdiReport job due to index ready : " + (indexReady(latest, currentIndexFinish, remoteFileSystem)) + " or resultDone : " + resultDone(remoteFileSystem, latest));
      }
    }catch(Exception ex) {
      System.out.println("failed to start xdiReport job " + ex.getMessage());
      ex.printStackTrace();
    }
    return null;
  }
  
  /**
   * Check if xdi result (eg. screen6) uploading finished or not
   * @param remoteFileSystem
   * @param latest
   * @return
   */
  private boolean resultDone(FileSystem remoteFileSystem, int latest) {
    try {
      Path donePath = new Path(resultPath + latest + "/screen6/done");//TODO, make /screen6/done configurable
      if(remoteFileSystem.exists(donePath)) {
        return true;
      }
      System.out.println("Cannot find done file");
    }catch(Exception ex) {
      System.out.println("Failed to find done file " + ex.getMessage());
    }
    return false;
  }
  
  /**
   * check if target date of index have been generated or not
   */
  private final static String STATUS_KEY = "DONE_XDI_INDEX_";
  private boolean indexReady(int latest, int currentIndexFinish, FileSystem remoteFileSystem) {
    try {
      Path currentIndex = new Path(statusPath + STATUS_KEY + latest);
      if(remoteFileSystem.exists(currentIndex)) {
        System.out.println("index for target date " + latest + " exist , " + currentIndex.toString());
        return true; 
      } else if(latest < currentIndexFinish) {
        System.out.println("index for target date " + latest + " < " + currentIndexFinish);
        return true; 
      } else {
        throw new Exception("index for target date " + latest + " not exist , " + currentIndex.toString());
      }
    }catch(Exception ex) {
      System.out.println("failed to find index status for target date : " + latest);
      ex.printStackTrace();
    }
    return false;
  }
  
  /**
   * remove status file under /user/dmp/xdi/result/, and update to a new status file
   * @return
   */
  public void end(String[] strStat) {
    FileSystem remoteFileSystem = null;
    try {
      if(strStat != null && strStat.length == 2) {
        String oldDate = strStat[0];
        String newDate = strStat[1];
        if(oldDate == null || oldDate.length() == 0)
          throw new Exception("old date is empty");
        if(newDate == null || newDate.length() == 0)
          throw new Exception("new date is empty");
        URI hdfsNamenode = new URI(hdfsURL);
        remoteFileSystem = FileSystem.get(hdfsNamenode, new Configuration());
        Path statusFile = new Path(statusPath + STATUS_XDI_KEY + oldDate);
        System.out.println("Delete remote status file " + statusFile.toString());
        remoteFileSystem.delete(statusFile, false);     
        Path newStatus = new Path(statusPath + STATUS_XDI_KEY + newDate);
        System.out.println("create new status file : " + newStatus.toString());
        remoteFileSystem.create(newStatus);
      }
    }catch(Exception ex) {
      System.out.println("failed to update stats file : " + ex.getMessage());
      ex.printStackTrace();
    }
  }
  
  public static void recursiveDelete(File file) {
    //to end the recursive loop
    if (!file.exists())
      return;
    //if directory, go inside and call recursively
    if (file.isDirectory()) {
      for (File f : file.listFiles()) {
        //call recursively
        recursiveDelete(f);
      }
    }
    System.out.println("--> delete file : " + file.toString());
    file.delete();
  }
}
