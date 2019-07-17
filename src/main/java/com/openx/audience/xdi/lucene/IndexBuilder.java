package com.openx.audience.xdi.lucene;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

public class IndexBuilder {

  public static void main(String[] args) {
    try {
      /*
			URI hdfsNamenode = new URI("hdfs://10.229.79.14:9000");
			FileSystem remoteFileSystem = FileSystem.get(hdfsNamenode, new Configuration());
			Path source = new Path("/user/hzhao/", "*");
			final FileStatus[] allFiles = remoteFileSystem.globStatus(source);
			for(FileStatus eachFile : allFiles) {
				if(eachFile.isDirectory()) {
					final FileStatus[] subAllFiles = remoteFileSystem.globStatus(new Path(eachFile.getPath().toString(), "*"));
					for(FileStatus eachSub : subAllFiles) {
						if(eachSub.isFile()) {
							System.out.println(eachSub.getPath().getName());
							Path dstPath = new Path("/Users/hzhao/work/testData");
							remoteFileSystem.copyToLocalFile(eachSub.getPath(), dstPath);
						}						
					}
				}
			}*/	
      //			int i = Integer.parseInt("08");
      //			new IndexBuilder().listPaths("m=08/d=28/h=14");
      URI hdfsUrl = new URI("hdfs://xvaic-sg-36.xv.dc.openx.org:8020"); //PROD
      //URI hdfsNamenode = new URI("hdfs://xvaa-i35.xv.dc.openx.org:8020");//QA
      //new IndexBuilder().findTargetPath("2015", "10", "24", hdfsNamenode);//Test
      int indexPartition = IndexConfig.getInstance().getPartitionSize();
      System.out.println("index partition : " + indexPartition);
      if(indexPartition < 1) {
        System.out.println("Failed to retrieve partition number");
        return;
      }
      String outputPath = "/var/tmp/stage"; //"/Users/hzhao/Documents/workspace/luceneTest/index/screen6.part"; TODO put to args
      //int taskNum = 1; //TODO put to args
      String localPath = "/var/tmp/source"; //"/Users/hzhao/Documents/workspace/luceneTest/prepare";  TODO put to args
      //String day_source = "/user/dmp/xdi/xdi-market-request/y=2015/m=10/d=18/";
      String day_source = "/user/dmp/xdi/xdi-market-request/y=<year>/m=<month>/d=<day>/";
      String lockUrl = "http://prod-cdh-grid-locks-rest.xv.dc.openx.org:8080/locks/query";
      String lockKey = "AG_XDI_EXPORT_COMPLETED";
      //String target = "";
      /*if(args != null && args.length > 5) {
			    hdfsNamenode = new URI(args[0]);
			    indexPartition = Integer.parseInt(args[1]);
			    outputPath = args[2];
			    taskNum = Integer.parseInt(args[3]);
			    localPath = args[4];
			    source = args[5];
			    System.out.println("[" + args[0] + " | " + args[1] + " | " + args[2] + " | " + args[3] + " | " + args[4] + " | " + args[5] + "]");
			}*/
      /*
      String year = null, month = null, day = null;
      if(args != null && args.length > 2) {
        year = args[0];
        if(year != null && year.length() > 0)
          day_source = day_source.replace("<year>", year);
        month = args[1];
        if(month != null && month.length() > 0)
          day_source = day_source.replace("<month>", month);
        day = args[2];
        if(day != null && day.length() > 0)
          day_source = day_source.replace("<day>", day);
      } else {
        System.out.println("missing parameters, must be 3 !");
        System.exit(0);
      }*/
      IndexBuilder indexBuilder = new IndexBuilder();
      Configuration hadoopConfig = new Configuration();
      hadoopConfig.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
      hadoopConfig.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
      FileSystem remoteFileSystem = FileSystem.get(hdfsUrl, hadoopConfig);
      
      String[] split = new String[4];
      day_source = indexBuilder.checkDate(day_source, split, remoteFileSystem);
      if(day_source == null || day_source.length() == 0) {
        System.out.println("failed to generate day_source !");
        System.exit(0);
      }
      System.out.println("day source is : " + day_source);
      String year = split[0], month = split[1], day = split[2];
      System.out.println("year : " + year + " month : " + month + " day : " + day);
      List<String> hours = indexBuilder.listHours(remoteFileSystem, day_source, split, lockUrl, lockKey);
      //indexBuilder.cutHours(hours, 2);
      int hourIndex = 0;
      for(String hourSource : hours) {
        BuilderGroupHandler bHandler = new BuilderGroupHandler(indexPartition, completePath(hourSource, outputPath));
        bHandler.startGroup();
        //taskNum based on group
        List<Path> paths = indexBuilder.findPaths(remoteFileSystem, hourSource, completePath(hourSource, localPath));
        //TODO : after download, delete hour data in tmp folder
        
        String currTime = null;
        int index = 0;
        int size = paths.size();
        List<Path> each = null;
        List<List<Path>> groupList = new ArrayList<List<Path>>();
        for(Path path : paths) {
          if(currTime == null) {
            each = new ArrayList<Path>();
            String[] strList = path.getName().split("-");
            currTime = strList[1];
          }
          each.add(path);
          if(index < size - 1) {
            Path nextPath = paths.get(index + 1);
            String[] strList = nextPath.getName().split("-");
            if(!currTime.equals(strList[1])) {
              currTime = null;
              groupList.add(each);
            }
          } else if(index == size - 1 && each.size() > 0) {
            groupList.add(each);
          }		
          index++;
        }
        // ----- parse file
        int gIndex = 0;
        for(List<Path> group : groupList) {
          int groupNum = group.size();
          System.out.println("parse group handle : " + groupNum + " at group : " + gIndex);
          ParserGroupHandler pHandler = new ParserGroupHandler(bHandler, groupNum, completePath(hourSource, localPath), remoteFileSystem);
          for(Path path : group) {
            System.out.println("add path : " + path.toString());
            pHandler.addPath(path);
          }
          pHandler.execute();
          gIndex++;
        }
        //  ----- finish build index
        String[] curHourSlot = indexBuilder.getHour(hourSource);
        System.out.println("send shutdwon to all build handler for hour " + curHourSlot[3]);
        try {
          //if(Integer.parseInt(curHourSlot[3]) == 23) {
          if(hourIndex == (hours.size() - 1) || (hourIndex % 2 == 1)) {
            System.out.println("end of batch or last hour of day or double index, shutdown with merge " + hourIndex);
            bHandler.shutdown(true);
          } else {
            System.out.println("soft shutdown for hour index : " + hourIndex);
            bHandler.shutdown(false);
          }
        }catch(InterruptedException ex) {
          System.out.println("Error when shutdown build handler thread");
          ex.printStackTrace();
        }
        String updatedStatus = curHourSlot[0] + "-" + curHourSlot[1] + "-" + curHourSlot[2] + "-" + curHourSlot[3];
        System.out.println("Finished build hourly index, update status with " + updatedStatus);
        indexBuilder.writeStatus(new Path(eStatus), updatedStatus, remoteFileSystem);
        indexBuilder.deleteLocalHourFolder(localPath, curHourSlot[0], curHourSlot[1], curHourSlot[2], curHourSlot[3]);
        System.out.println("shutdwon all build handler for hour " + curHourSlot[3] + " complete !!!");
        hourIndex++;
      }
      if(hours.size() == 0) {
        System.out.println("Hour size is 0");
        return;
      }
      String[] lastSlot = indexBuilder.getLastHour(hours);
      if(Integer.parseInt(lastSlot[3]) == 23) {
        System.out.println("<================ shutdown completely ================>");
        System.out.println("copy index to HDFS");
        Path targetIndexSlot = indexBuilder.findTargetPath(year, month, day, remoteFileSystem);
        System.out.println("remote index slot : " + targetIndexSlot.toString());
        File stageFile = new File(outputPath + "/" + (year + month + day));
        if(stageFile.exists() && stageFile.isDirectory()) {
          File[] parts = stageFile.listFiles();
          for(File eachPart : parts) {
            if(!eachPart.getName().startsWith("part_"))
              continue;
            Path remotePath = new Path(targetIndexSlot.toString() + "/" + year + month + day + "/" + eachPart.getName());
            System.out.println("create remote index date folder : " + remotePath.toString());
            remoteFileSystem.mkdirs(remotePath);
            File[] indexFiles = eachPart.listFiles();
            for(File eachIndexFile : indexFiles) {
              if(eachIndexFile.getName().endsWith(".crc")) {
                System.out.println("delete local file " + eachIndexFile.getAbsolutePath() + " to " + remotePath.toString());
                //remoteFileSystem.copyFromLocalFile(false, true, new Path(eachIndexFile.getAbsolutePath()), remotePath);
                eachIndexFile.delete();
              }
            }
            indexFiles = eachPart.listFiles();
            for(File eachIndexFile : indexFiles) {
              if(!eachIndexFile.getName().endsWith(".crc")) {
                System.out.println("copy local file " + eachIndexFile.getAbsolutePath() + " to " + remotePath.toString());
                remoteFileSystem.copyFromLocalFile(false, true, new Path(eachIndexFile.getAbsolutePath()), remotePath);
              }
            }
          }
        }
        System.out.println("copy index done, check compelete and write status");
        indexBuilder.checkStatus(outputPath, (year + month + day), indexPartition);
        new ReplaceIndex().checkAndReplace((year + month + day), remoteFileSystem);
        indexBuilder.deleteLocalDayArchive(outputPath, year, month, day);
      }
    }catch(Exception ex) {
      ex.printStackTrace();
    }finally {
      System.out.println("Ready to shutdown in 5s");
      try {
        Thread.currentThread().sleep(5000);
      }catch(Exception ex) {ex.printStackTrace();}
      System.exit(0);
    }
  }
  
  private String[] getLastHour(List<String> hours) {
    String last = hours.get(hours.size() - 1);
    return this.getHour(last);
  }
  
  private String[] getHour(String hourPath) {
    String[] slots = new String[4];
    String[] splits = hourPath.split("/");
    for(String each : splits) {
      if(each.startsWith("y="))
        slots[0] = each.substring(each.indexOf("y=") + 2);
      if(each.startsWith("m="))
        slots[1] = each.substring(each.indexOf("m=") + 2);
      if(each.startsWith("d="))
        slots[2] = each.substring(each.indexOf("d=") + 2);
      if(each.startsWith("h="))
        slots[3] = each.substring(each.indexOf("h=") + 2);
    }
    return slots;
  }
  
  private final static String localStagePath = "/var/tmp/stage";
  private static final String STATUS_LOCAL_INDEX_KEY = "DONE_INDEX_ARCHIVE_";
  private String checkDate(String day_source, String[] split, FileSystem remoteFileSystem) throws Exception{
    File stagePath = new File(localStagePath);
    if(stagePath.isDirectory()) {
      File[] dates = stagePath.listFiles();
      List<Integer> iDates = new ArrayList<Integer>();
      for(File date : dates) {
        iDates.add(Integer.parseInt(date.getName()));
      }
      Collections.sort(iDates, Collections.reverseOrder());
      //String latestDate = Integer.toString(iDates.get(0));
      int lastDate = iDates.get(0);
      System.out.println("check latest date path : " + localStagePath + "/" + lastDate);
      File currentPath = new File(localStagePath + "/" + lastDate);
      File currentStatuFile = new File(localStagePath + "/" + lastDate + "/"+ STATUS_LOCAL_INDEX_KEY + lastDate);
      int temp = lastDate;
      int _y = temp / 10000;
      temp = temp % 10000;
      int _m = temp / 100;
      int _d = temp % 100;
      if(currentStatuFile.exists()) {
        String tempTime = _y + "-" + _m + "-" + _d;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar c = Calendar.getInstance();
        c.setTime(sdf.parse(tempTime));
        c.add(Calendar.DATE, 1);
        tempTime = sdf.format(c.getTime());
        String[] newDataSlot = tempTime.split("-");     
        if(_y > 0) {
          day_source = day_source.replace("<year>", newDataSlot[0]);
          split[0] = newDataSlot[0];
        }
        if(_m > 0) {
          day_source = day_source.replace("<month>", newDataSlot[1]);
          split[1] = newDataSlot[1];
        }
        if(_d > 0) {
          day_source = day_source.replace("<day>", newDataSlot[2]);
          split[2] = newDataSlot[2];
        }
        split[3] = "00";
        return day_source;
      } else if (currentPath.exists()){
        String[] status = this.findCurrentHour(remoteFileSystem); //Read Current Finished year,month,day,hour
        this.addHour(status);    
        day_source = day_source.replace("<year>", status[0]);
        split[0] = status[0];
        day_source = day_source.replace("<month>", status[1]);
        split[1] = status[1];
        day_source = day_source.replace("<day>", status[2]);
        split[2] = status[2];
        split[3] = status[3];
        return day_source;
      }
    }
    return null;
  }
  
  private void addHour(String[] status) throws ParseException {
    String tempTime = status[0] + "-" + status[1] + "-" + status[2];
    String strHour = status[3];
    if(Integer.parseInt(strHour) < 23) {
      int addHour = Integer.parseInt(strHour) + 1;
      if(addHour < 10)
        strHour = "0" + addHour;
      else
        strHour = Integer.toString(addHour);
      status[3] = strHour;
    } else if(Integer.parseInt(strHour) == 23) {
      status[3] = "00";
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
      Calendar c = Calendar.getInstance();
      c.setTime(sdf.parse(tempTime));
      c.add(Calendar.DATE, 1);
      tempTime = sdf.format(c.getTime());
      String[] newDataSlot = tempTime.split("-");
      status[0] = newDataSlot[0];
      status[1] = newDataSlot[1];
      status[2] = newDataSlot[2];
    }
  }
  
  private final static String eStatus = "/user/dmp/xdi/index/status/e_status.txt";
  private String[] findCurrentHour(FileSystem remoteFileSystem) throws Exception {
    String res = this.readStatus(new Path(eStatus), remoteFileSystem);
    if(res == null)
      throw new Exception("Failed to get e_status");
    String[] exeStatus = res.split("-");
    if(exeStatus.length != 4)
      throw new Exception("Wrong e_status format");
    return exeStatus;
  }
  
  private String readStatus(Path source, FileSystem remoteFileSystem){
    BufferedReader br = null;
    try {
      br = new BufferedReader(new InputStreamReader(remoteFileSystem.open(source)));
      String line = br.readLine();
      return line;
    }catch(IOException ex) {
      ex.printStackTrace();
    }finally {
      if(br != null) {
        try {
          br.close();
        }catch(IOException ex) {
          ex.printStackTrace();
        }
      }
    }
    return null;
  }
  
  private void writeStatus(Path statusFile, String updatedStatus, FileSystem remoteFileSystem) throws Exception {
    if(remoteFileSystem.exists(statusFile)) {
      remoteFileSystem.delete(statusFile, false);
      if(remoteFileSystem.exists(statusFile)) {
        throw new Exception("Failed to overwrite status file");
      }
    }
    BufferedWriter br = null;
    try {
      br = new BufferedWriter(new OutputStreamWriter(remoteFileSystem.create(statusFile,true)));
      br.write(updatedStatus);
    } finally {
      br.close();
    }
  }

  private boolean deleteLocalHourFolder(String localPath, String year, String month, String day, String hour) {
    String path = localPath + "/" + year + "/" + month + "/" + day + "/" + hour;
    File file = new File(path);
    if(file.exists()) {
      System.out.println("delete local hour folder : " + path);
      recursiveDelete(file);
    }
    if(!file.exists()) return true;
    else return false;
  }

  private boolean deleteLocalDayArchive(String stagePath, String year, String month, String day) {
    String path = stagePath + "/" + year + month + day;
    File file = new File(path);
    if(file.exists()) {
      System.out.println("prepare delete local day archive : " + path);
      File[] files = file.listFiles();
      for(File each : files) {
        if(each.getName().startsWith("DONE_part") && each.isFile()) {
          System.out.println("delete " + each.toString());
          each.delete();
        }
        if(each.getName().startsWith("part_") && each.isDirectory()) {
          System.out.println("delete " + each.toString());
          recursiveDelete(each);
        }
      }
    }
    if(!file.exists()) return true;
    else return false;
  }

  /*
  private String findNextHour(File currentPath) throws Exception {
    File[] hours = currentPath.listFiles();
    if(hours == null || hours.length == 0)
      return "00";
    List<String> hList = new ArrayList<String>();
    for(File hour : hours) {
      hList.add(hour.getName());
    }
    Collections.sort(hList, Collections.reverseOrder());
    String nextHour = null;
    int curr = Integer.parseInt(hList.get(0));
    if (curr >= 23)
      throw new Exception("Next hour is over 23");
    int next =  curr + 1;
    if(next < 10) 
      nextHour = "0" + next;
    return nextHour;
  }*/
  
  private void cutHours(List<String> hours, int firstNum) {
    int index = 0;
    Iterator<String> iter = hours.iterator();
    while(iter.hasNext()) {
      String cur = iter.next();
      if(index >= firstNum) {
        iter.remove();
        System.out.println("remove " + cur);
      } else {
        System.out.println("keep " + cur);
      }
      index++;
    }
    System.out.println("final hour size after cut is " + hours.size());
  }

  private static final String remoteIndexPath = "/user/dmp/xdi/index/";
  private void checkStatus(String outputPath, String currentDate, int indexPartition) {
    boolean allDone = true;
    for(int i = 0; i < indexPartition; i++) {
      String fileName = outputPath + "/" + currentDate + "/DONE_part_"+i+"_"+currentDate+"23";
      File statusFile = new File(fileName);
      if(!statusFile.exists()) {
        System.out.println("Last status file do not exist, please check index generate !!!");
        allDone = false;
        break;
      } else {
        System.out.println(fileName + " exist");
      }
    }
    if(allDone) {
      try {
        File statusFile = new File(outputPath + "/" + currentDate + "/DONE_INDEX_ARCHIVE_"+currentDate);
        statusFile.createNewFile();
        //System.out.println("create index archive success status file " + statusFile.getName() + " at " + remoteIndexPath);
        //remoteFileSystem.copyFromLocalFile(false, true, new Path(statusFile.getAbsolutePath()), new Path(remoteIndexPath));
      }catch(Exception ex) {
        System.out.println("Failed to create remote status file");
      }
    }
  }

  /**
   * @param dayPath: eg. /user/dmp/xdi/xdi-market-request/y=2015/m=10/d=07/
   * @return
   * @throws IOException
   */
  /*
  public List<String> listHours(URI hdfsNamenode, String dayPath) throws IOException {
    FileSystem remoteFileSystem = FileSystem.get(hdfsNamenode, new Configuration());
    List<String> hours = new ArrayList<String>();
    Path daysource = new Path(dayPath, "*");
    final FileStatus[] allFiles = remoteFileSystem.globStatus(daysource);
    for(FileStatus eachFile : allFiles) {
      if(eachFile.isDirectory()) {
        String eachHour = eachFile.getPath().toString();
        hours.add(eachHour);
      }
    }
    return hours;
  }*/
  
  public List<String> listHours(FileSystem remoteFileSystem, String dayPath, String[] splits, String url, String key) throws Exception {
    List<Path> hourList = new ArrayList<Path>();
    Path daysource = new Path(dayPath, "*");
    final FileStatus[] allFiles = remoteFileSystem.globStatus(daysource);
    for(FileStatus eachFile : allFiles) {
      if(eachFile.isDirectory()) {
        Path eachHour = eachFile.getPath();
        hourList.add(eachHour);
      }
    }
    Collections.sort(hourList, new Comparator<Path>(){
      @Override
      public int compare(Path p1, Path p2) {
        String str1 = p1.getName();
        String str2 = p2.getName();
        String[] c_str1 = str1.split("=");
        String[] c_str2 = str2.split("=");
        return (int)(Long.parseLong(c_str1[1]) - Long.parseLong(c_str2[1]));
      }
    });
//    if(!sourceReady(hourList)) {
//      throw new Exception("Data Loading for [" + dayPath + "] is not complete yet !");
//    }
    int readyHour = this.getReadyHour(splits, url, key);
    System.out.println("lock url data ready hour : " + readyHour + " ready to build hour : " + splits[3]);
    List<String> hours = new ArrayList<String>();
    for(Path hour : hourList) {
      String strHour = hour.getName();
      int iHour = Integer.parseInt(strHour.split("=")[1]);
      if(iHour >= Integer.parseInt(splits[3]) && iHour <= readyHour) {
        hours.add(hour.toString());
        System.out.println("Ready copy " + hour.toString() + " to local");
      }
    }
    return hours;
  }
  
  private int getReadyHour(String[] splits, String lockUrl, String lockKey) throws Exception {
    String strUrl = lockUrl + "/" + lockKey;
    URL url = new URL(strUrl);
    URLConnection connection = url.openConnection();
    BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
    String line = "", data = "";
    while ((line = reader.readLine()) != null) {
      data += line;
    }
    if(data == null || data.length() == 0)
      throw new Exception("Failed to get lock url");
    String time = this.parseLockXml(data, lockKey);
    String[] dh = time.split("T");
    String[] day = dh[0].split("-");
    if(day.length == 3) {
      /*
      if(!day[0].equals(splits[0]) || !day[1].equals(splits[1]) || !day[2].equals(splits[2])) {
        if(Integer.parseInt(splits[2]) < Integer.parseInt(day[2]))
          return 23;
        throw new Exception("Day unmatch year : {" + day[0] + " | "+ splits[0]+ "} month : {" + day[1] + " | " + splits[1] +"} day : {" + day[2] + " | " + splits[2] + "}");
      }*/
      System.out.println("lock url compare to local year : {" + day[0] + " | "+ splits[0]+ "} month : {" + day[1] + " | " + splits[1] +"} day : {" + day[2] + " | " + splits[2] + "}");
      if(day[0].equals(splits[0]) && day[1].equals(splits[1]) && day[2].equals(splits[2])) {
        System.out.println("Current Day match to lock url");
      } else if(day[0].equals(splits[0]) && day[1].equals(splits[1]) && !day[2].equals(splits[2])) {
        if(Integer.parseInt(splits[2]) < Integer.parseInt(day[2])) {
          System.out.println("Local day older than lock url");
          return 23;
        } else {
          throw new Exception("Local day newer than lock url");
        }
      } else if(day[0].equals(splits[0]) && !day[1].equals(splits[1]) && !day[2].equals(splits[2])) {
        if(Integer.parseInt(day[1]) - Integer.parseInt(splits[1]) == 1) {
          System.out.println("New day in next Month");
          return 23;
        } else {
          throw new Exception("Local day newer than lock url");
        }
      } else if(!day[0].equals(splits[0]) && !day[1].equals(splits[1]) && !day[2].equals(splits[2])) {
        if(Integer.parseInt(day[0]) - Integer.parseInt(splits[0]) == 1) {
          System.out.println("New day in next Year");
          return 23;
        } else {
          throw new Exception("Local day newer than lock url");
        }
      } else {
        throw new Exception("Failed to get ready hour, maybe way old than lock url");
      }
    } else {
      throw new Exception("illegal day format");
    }
    String hour = dh[1].substring(0, dh[1].indexOf(":"));
    return Integer.parseInt(hour);
  }
  
  private String parseLockXml(String data, String lockKey) throws Exception {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = factory.newDocumentBuilder();
    Document doc = builder.parse(new InputSource(new StringReader(data)));
    doc.getDocumentElement().normalize();
    NodeList dList = doc.getElementsByTagName("Lock");
    for(int i = 0; i < dList.getLength(); i++) {
      Element element = (Element)dList.item(i);
      String name = element.getElementsByTagName("name").item(0).getTextContent();
      if(name.equals(lockKey)) {
        String time = element.getElementsByTagName("time").item(0).getTextContent();
        return time;
      }
    }
    return null;
  }
  
  private boolean sourceReady(List<Path> hours) {
    Path lastHour = hours.get(hours.size() - 1);
    String lastHourName = lastHour.getName();
    if(lastHourName.equals("h=23"))
      return true;
    System.out.println("WARN: last hour is : " + lastHourName);
    return false;
  }

  /*
   * return /var/tmp/stage/2015/10/07/18 or /var/tmp/source/2015/10/07/18
   */
  private static String completePath(String source, String target) {
    String token = "/user/dmp/xdi/xdi-market-request/";
    String output = source.substring(source.indexOf(token) + token.length());
    String result = "";
    String[] split = output.split("/");
    //[y=2015, m=10, d=06, h=00]
    for(String each : split) {
      result += each.substring(each.indexOf("=") + 1) + "/";
    }
    return target + "/" + result;
  }

  //TODO : change it to parallel downloading [necessary??]
  public List<Path> findPaths(FileSystem remoteFileSystem, String remoteFolder, String destLocalPath) throws IOException {
    List<Path> targets = new ArrayList<Path>();
    LoadGroupHandler gHandler = new LoadGroupHandler(remoteFileSystem, 20, destLocalPath);
    Path source = new Path(remoteFolder, "*");
    final FileStatus[] allFiles = remoteFileSystem.globStatus(source);
    for(FileStatus eachFile : allFiles) {
      if(eachFile.isFile()) {
        String eachFileName = eachFile.getPath().getName();
        Path destPath = new Path(destLocalPath + "/" + eachFileName);
        System.out.println(" ======>1 add " + eachFile.getPath().toString() + " into copy list");
        gHandler.addPath(eachFile.getPath());
        targets.add(destPath);
      } else {
        FileStatus[] files = remoteFileSystem.listStatus(eachFile.getPath());
        for(FileStatus each : files) {
          if(each.isFile()) {
            String eachName = each.getPath().getName();
            Path destPath = new Path(destLocalPath + "/" + eachName);
            System.out.println(" ======>2 add " + each.getPath().toString() + " into copy list");
            gHandler.addPath(each.getPath());
            targets.add(destPath);
          }
        }
      }
    }
    gHandler.execute();
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
    return targets;
  }

  private static final String archivePath = "/user/dmp/xdi/index/archive/";
  public Path findTargetPath(String year, String month, String day, FileSystem remoteFileSystem) throws IOException {
    Path result = null;
    String target = year+month+day;
    Path source = new Path(archivePath, "*");
    final FileStatus[] allFiles = remoteFileSystem.globStatus(source);
    List<Integer> paths = new ArrayList<Integer>();
    for(FileStatus eachFile : allFiles) {
      String eachFileName = eachFile.getPath().getName();
      paths.add(Integer.parseInt(eachFileName));
    }
    Collections.sort(paths);
    int last = paths.get(paths.size() - 1);
    if(Integer.parseInt(target) > last) {
      try {
        int temp = last;
        int _y = temp / 10000;
        temp = temp % 10000;
        int _m = temp / 100;
        int _d = temp % 100;
        String tempTime = _y + "-" + _m + "-" + _d;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar c = Calendar.getInstance();
        c.setTime(sdf.parse(tempTime));
        c.add(Calendar.DATE, IndexConfig.getInstance().getSlotSize());
        tempTime = sdf.format(c.getTime());
        String[] newDataSlot = tempTime.split("-");
        String newData = newDataSlot[0] + newDataSlot[1] + newDataSlot[2];
        if(Integer.parseInt(newData) >= Integer.parseInt(target)) {
          result = new Path(archivePath + newData);
          System.out.println("create remote date slot : " + result.toString());
          remoteFileSystem.mkdirs(result);
        } else {
          throw new Exception("target date is over 6 days");
        }
      }catch(Exception ex) {
        System.out.println("Failed to create new date slot "  + ex.getMessage());
        ex.printStackTrace();
      }
    } else {
      for(Integer pathInt : paths) {
        if(Integer.parseInt(target) <= pathInt) {
          result = new Path(archivePath + pathInt);
          break;
        }
      }
    }
    return result;
  }

  /*
    private static final String hdfsNamenodeURL = "hdfs://10.229.79.14:9000";
    private static final String remoteIndexPath = "/user/hzhao/dma-803/index/";
    private static final String localIndexPath = "/Users/hzhao/work/test/index/";
	public void test() {
		URI hdfsNamenode = null;
		try {
			hdfsNamenode = new URI(hdfsNamenodeURL);		
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		try {
			FileSystem remoteFileSystem = FileSystem.get(hdfsNamenode, new Configuration());
			Path source = new Path(remoteIndexPath, "*");
			final FileStatus[] allFiles = remoteFileSystem.globStatus(source);
			for(FileStatus eachFile : allFiles) {
				Path remotePath = eachFile.getPath();
				if(remotePath != null) {
					System.out.println("-- remote index file path : " + remotePath.toString());
					String fileName = remotePath.getName();
					File p = new File(localIndexPath);
					if(!p.exists()) {
						p.mkdirs();
						System.out.println("create folder : " + p.getAbsolutePath());
					}
					File f = new File(localIndexPath + "/" + fileName);
					System.out.println("-- copy remote index file path : " + remotePath.toString() + " to local index path " + f.getAbsolutePath());
					if(f.exists()) {
						//f.delete();
						System.out.println("-- local index file exist, do nothing");
					} else {
						Path localPath = new Path(localIndexPath);//dest
						System.out.println(" ======> copy from HDFS: " + eachFile.getPath().toString() + " to local : " + localPath.toString());				
						remoteFileSystem.copyToLocalFile(remotePath, localPath);
					}
				} else {
					System.out.println("-- remote index file path is null");
				}
			}
		}catch(Exception ex) {
			System.out.println("-- error when setup " + ex.getMessage());
			ex.printStackTrace();
		}
	}*/

  private static final String hdfsURL = "hdfs://xvaic-sg-36.xv.dc.openx.org:8022";//PROD
  //private static final String hdfsURL = "hdfs://xvaa-i35.xv.dc.openx.org:8020";//QA
  private static final String xdiBasePath = "/user/dmp/xdi/xdi-market-request/y=2015/";
  private static final String xdiOutPath = "/user/dmp/xdi/output/index_";
  private void listPaths(String start) {
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
        if(this.convertToInteger(eachPath) > this.convertToInteger(start)) {
          array[0] = xdiBasePath + eachPath;
          array[1] = xdiOutPath + this.convertToInteger(eachPath);
          array[2] = ""+this.convertToInteger(eachPath);
          array[3] = ""+this.convertToInteger(previous);
          result.add(array);
          previous = eachPath;
        }
      }
      System.out.println("paths.size : " + result.size());
      System.out.println("============================");
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
  }

  private int convertToInteger(String path) {
    String[] s_str = path.split("/");
    String c_str = "";
    for(String sub : s_str) {
      c_str += sub.substring(sub.indexOf("=") + 1);
    }
    return Integer.parseInt(c_str);
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
    file.delete();
  }
}
