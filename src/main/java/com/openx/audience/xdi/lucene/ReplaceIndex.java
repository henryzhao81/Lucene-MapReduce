package com.openx.audience.xdi.lucene;

import java.io.File;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ReplaceIndex {

  public static void main(String[] args) {
    new ReplaceIndex().checkAndReplace();
  }
  
  private final static String localStagePath = "/var/tmp/stage";
  private static final String hdfsURL = "hdfs://xvaic-sg-36.xv.dc.openx.org:8022";//PROD
  //private static final String hdfsURL = "hdfs://xvaa-i35.xv.dc.openx.org:8020";//QA
  private static final int indexPartition = 11;
  private final static String STATUS_KEY = "DONE_XDI_INDEX_";
  private static final String STATUS_LOCAL_INDEX_KEY = "DONE_INDEX_ARCHIVE_";
  private static final String statusPath = "/user/dmp/xdi/index/status/";

  public void checkAndReplace(String newDate, FileSystem remoteFileSystem) throws Exception {
    File stagePath = new File(localStagePath);
    if(stagePath.isDirectory()) {
      File[] dates = stagePath.listFiles();
      List<Integer> iDates = new ArrayList<Integer>();
      for(File date : dates) {
        iDates.add(Integer.parseInt(date.getName()));
      }
      Collections.sort(iDates, Collections.reverseOrder());
      String latestDate = Integer.toString(iDates.get(0));
      System.out.println("check latest date path : " + localStagePath + "/" + latestDate);
      File latestPath = new File(localStagePath + "/" + latestDate + "/"+ STATUS_LOCAL_INDEX_KEY + latestDate);
      if(latestPath.exists()) {
        this.removeExistStatus(remoteFileSystem);
        Path rNewPath = new Path(statusPath + STATUS_KEY + newDate);
        System.out.println("create new status file : " + rNewPath.toString());
        remoteFileSystem.create(rNewPath);
      }
    }
  }
  
  //Remove whatever existing DONE_XDI_INDEX_? it is
  private void removeExistStatus(FileSystem remoteFileSystem) throws Exception{
    Path status = new Path(statusPath, "*");
    final FileStatus[] statusFiles = remoteFileSystem.globStatus(status);
    if(statusFiles != null && statusFiles.length > 0) {
      Path dPath = null;
      for(FileStatus stat : statusFiles) {
        String statName = stat.getPath().getName();
        if(statName.startsWith(STATUS_KEY)) {
          dPath = stat.getPath();
          break;
        }
      }
      if(dPath != null) {
        System.out.println("Delete remote status file " + dPath.toString());
        remoteFileSystem.delete(dPath, false);
      }
    }
  }

  public void checkAndReplace() {
    File stagePath = new File(localStagePath);
    if(stagePath.isDirectory()) {
      File[] dates = stagePath.listFiles();
      List<Integer> iDates = new ArrayList<Integer>();
      for(File date : dates) {
        iDates.add(Integer.parseInt(date.getName()));
      }
      Collections.sort(iDates, Collections.reverseOrder());
      String latestDate = Integer.toString(iDates.get(0));
      System.out.println("check latest date path : " + localStagePath + "/" + latestDate);
      File latestPath = new File(localStagePath + "/" + latestDate + "/"+ STATUS_LOCAL_INDEX_KEY + latestDate);
      if(latestPath.exists()) {
        try {
          URI hdfsNamenode = new URI(hdfsURL);
          FileSystem remoteFileSystem = FileSystem.get(hdfsNamenode, new Configuration());
          boolean success = this.removeTarget(remoteFileSystem, latestDate);
          boolean copySuccess = false;
          if(success) {
            copySuccess = this.copyTarget(remoteFileSystem, latestDate);
            if(copySuccess) {
              String newStatusFileName = STATUS_KEY + latestDate;
              Path newStatus = new Path(statusPath + newStatusFileName);
              System.out.println("create new status file : " + newStatus.toString());
              remoteFileSystem.create(newStatus);
            }
          }
        }catch(Exception ex) {
          System.out.println("Failed to replace index " + ex.getMessage());
          ex.printStackTrace();
        }
      } else {
        System.out.println("[WARN] Index for latest date " + latestDate + " have not finished yet, replace index later until index build finished !");
      }
    }
  }

  private static final String indexPath_0 = "/user/dmp/xdi/index/part_0/";
  private String findTargetSlot(FileSystem remoteFileSystem, String target) throws Exception {
    Path source = new Path(indexPath_0, "*");
    final FileStatus[] allFiles = remoteFileSystem.globStatus(source);
    List<Integer> paths = new ArrayList<Integer>();
    for(FileStatus eachFile : allFiles) {
      String eachFileName = eachFile.getPath().getName();
      paths.add(Integer.parseInt(eachFileName));
    }
    Collections.sort(paths);
    int last = paths.get(paths.size() - 1);
    if(Integer.parseInt(target) > last) {
      String newSlot = this.createNewSlot(remoteFileSystem, target, last);
      System.out.println("Cannot find target slot, create new slot " + newSlot);
      return newSlot;
    } else {
      for(Integer pathInt : paths) {
        if(Integer.parseInt(target) <= pathInt) {
          return Integer.toString(pathInt);
        }
      }
    }
    return null;
  }
  
  private String createNewSlot(FileSystem remoteFileSystem, String target, int last) throws Exception {
    int temp = last;
    int _y = temp / 10000;
    temp = temp % 10000;
    int _m = temp / 100;
    int _d = temp % 100;
    String tempTime = _y + "-" + _m + "-" + _d;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    Calendar c = Calendar.getInstance();
    c.setTime(sdf.parse(tempTime));
    c.add(Calendar.DATE, 6);
    tempTime = sdf.format(c.getTime());
    String[] newDataSlot = tempTime.split("-");
    String newData = newDataSlot[0] + newDataSlot[1] + newDataSlot[2];
    if(Integer.parseInt(newData) >= Integer.parseInt(target)) {
      return newData;
    } else {
      throw new Exception("target date is over 6 days");
    }
  }
  
  private static final String indexPath = "/user/dmp/xdi/index/part_";
  //private static final String indexPath = "/user/dmp/xdi/output/xdi_del/index/part_";
  private boolean removeTarget(FileSystem remoteFileSystem, String latestDate) throws Exception {
    String[] strStat = new String[1];
    if(isRemovable(remoteFileSystem, strStat, latestDate)) {
      String targetIndexSlot = this.findTargetSlot(remoteFileSystem, latestDate);
      if(targetIndexSlot == null)
        throw new Exception("Cannot find target index slot !");
      for(int i = 0 ; i < indexPartition; i++) {
        String removeIndexPath = indexPath + i + "/" + targetIndexSlot;
        if(!remoteFileSystem.exists(new Path(removeIndexPath))) {
          remoteFileSystem.mkdirs(new Path(removeIndexPath));
        } else {
          Path eachPart = new Path(removeIndexPath, "*");
          final FileStatus[] indexFiles = remoteFileSystem.globStatus(eachPart);
          for(FileStatus indexFile : indexFiles) {
            Path eachPartIndexFile = indexFile.getPath();
            System.out.println("remote index file : " + eachPartIndexFile.getName() + " in " + removeIndexPath);
            remoteFileSystem.delete(eachPartIndexFile, false);
          }
        }
      }
      if(strStat[0] != null && strStat[0].length() > 0) {
        Path statusFile = new Path(statusPath + strStat[0]);
        System.out.println("Delete remote status file " + statusFile.toString());
        remoteFileSystem.delete(statusFile, false);
        return true;
      } else
        return false;
    } else
      return false;
  }
  
  private boolean copyTarget(FileSystem remoteFileSystem, String latestDate) throws Exception{
    String targetIndexSlot = this.findTargetSlot(remoteFileSystem, latestDate);
    for(int i = 0 ; i < indexPartition; i++) {
      String removeIndexPath = indexPath + i + "/" + targetIndexSlot;
      Path eachPart = new Path(removeIndexPath, "*");
      final FileStatus[] indexFiles = remoteFileSystem.globStatus(eachPart);
      if(indexFiles.length == 0) {
        System.out.println("prepare copy from local " + (localStagePath+"/"+latestDate) + " at index part_" + i);
        this.copyEach(remoteFileSystem, removeIndexPath, (localStagePath+"/"+latestDate), i);
      } else {
        throw new Exception("Path " + removeIndexPath + " is not empty, make sure it is clean path !");
      }
    }
    return true;
  }
  
  private void copyEach(FileSystem remoteFileSystem, String targetRemote, String source, int index) throws Exception {
    System.out.println("target remote folder "+targetRemote+" is empty, ok to copy");
    File stagePart = new File(source + "/part_"+index);
    if(stagePart.exists()) {
       if(stagePart.isDirectory()) {
         File[] indexFiles = stagePart.listFiles();
         for(File eachIndexFile : indexFiles) {
           if(!eachIndexFile.getName().endsWith(".crc")) {
             System.out.println("copy local file " + eachIndexFile.getAbsolutePath() + " to " + targetRemote);
             remoteFileSystem.copyFromLocalFile(false, true, new Path(eachIndexFile.getAbsolutePath()), new Path(targetRemote));
           }
         }
       }
    }else {
      throw new Exception("Folder " + source + "/part_"+index + " do not exist, please check index generation");
    }
  }

  private boolean isRemovable(FileSystem remoteFileSystem, String[] strStat, String target) throws Exception {
    Path status = new Path(statusPath, "*");
    final FileStatus[] statusFiles = remoteFileSystem.globStatus(status);
    if(statusFiles != null && statusFiles.length > 0) {
      for(FileStatus stat : statusFiles) {
        String statName = stat.getPath().getName();
        if(statName.startsWith(STATUS_KEY)) {
          String completeDate = statName.substring(statName.indexOf(STATUS_KEY) + STATUS_KEY.length());
          if(Integer.parseInt(completeDate) == Integer.parseInt(target)) {
            throw new Exception("Index for " +  target + " was replaced already !");
          } else if(Integer.parseInt(completeDate) > Integer.parseInt(target)) {
            throw new Exception("Index for " + target + " is older than completed date " + completeDate);
          }
          if(!this.currentIndexFinished(completeDate, remoteFileSystem)) {
            System.out.println("Index for previous date " + completeDate + " have not finished processed by xdiReport yet, wait until process finished !");
            return false;
          }
          strStat[0] = statName;
        }
      }
    }else {
      System.out.println("Cannot find status file !");
      return false;
    }
    if(strStat[0] == null || strStat[0].length() == 0)
      return false;
    return true;
  }
  
  private static final String STATUS_XDI_KEY = "DONE_XDI_REPORT_";
  private boolean currentIndexFinished(String completeDate, FileSystem remoteFileSystem) {
    try {
      Path currentIndex = new Path(statusPath + STATUS_XDI_KEY + completeDate);
      if(remoteFileSystem.exists(currentIndex)) {
        System.out.println("index for date " + completeDate + " finished, " + currentIndex.toString());
        return true; 
      } else {
        throw new Exception("index for date " + completeDate + " not finished , " + currentIndex.toString());
      }
    }catch(Exception ex) {
      System.out.println("failed to find index status for date : " + completeDate);
      ex.printStackTrace();
    }
    return false;

  }
}
