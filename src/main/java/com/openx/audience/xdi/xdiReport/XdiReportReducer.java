package com.openx.audience.xdi.xdiReport;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.CompressionTools;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;

public class XdiReportReducer extends Reducer<Text, Text, Text, Text> {

  private Text result = new Text();
  private Text resKey = new Text();
  //private IndexReader reader = null;
  //private IndexSearcher searcher = null;
  private IndexReader[] readers = null;
  private IndexSearcher[] searchers = null;
  private Analyzer analyzer = null;
  private MultipleOutputs<Text, Text> mos;
  private String resultOutput;
  private String remainOutput;

  /**
   * index structure (HDFS)
   *                                 |--_xxx.fdt
   *                    |--20150928--|--_xxx.fdx  (6 days index, from 0923 ~ 0928)
   *         |--part_0--|--20151004  |-- ... (8 more)
   *         |          |--  ...
   *         |                       |--_xxx.fdt
   * index --|          |--20150928--|--_xxx.fdx
   *         |--part_1--|--20151004  |-- ... (8 more)
   *         |          |--  ...      
   *         |
   *         |-- ...    
   * 
   * index structure (Local) 
   *                                |--_xxx.fdt
   *                   |--20150928--|--_xxx.fdx  (6 days index, from 0923 ~ 0928)
   * index -- part_x --|            |-- ... (8 more)
   *                   |--20151004-- ...
   *                      
   * local may contain multiple partitions if not scale well in YARN cluster                  
   */

  private static final String hdfsNamenodeURL = "hdfs://xvaic-sg-36.xv.dc.openx.org:8020";//PROD
  //private static final String hdfsNamenodeURL = "hdfs://xvaa-i35.xv.dc.openx.org:8020";//QA
  private static final String remoteIndexPath = "/user/dmp/xdi/index/";
  private static final String localIndexPath = "/var/tmp/dmp/index/";

  @Override
  protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException, InterruptedException {
    mos = new MultipleOutputs(context);
    int reduceTaskId = context.getTaskAttemptID().getTaskID().getId(); 
    String jobId = context.getJobID().toString();
    String sourceIndex = context.getConfiguration().get("target_index");
    resultOutput = context.getConfiguration().get("result_output");
    remainOutput = context.getConfiguration().get("remain_output");
    String curretDate = context.getConfiguration().get("current_date");
    System.out.println("======== setup XdiReportReducer at task : " + reduceTaskId + " job id : " + jobId);
    //Delete All under index folder 
    //use global variable
    try {
      File localIndex = new File(localIndexPath);//localIndexPath : /var/tmp/dmp/index/
      File temp = new File(localIndexPath + jobId);
      if(localIndex.exists()) {
        //create temp job id folder
        if(!temp.exists()) {
          System.out.println("======== job id file " + (localIndexPath + jobId) + " does not exist, delete all " + " at task : " + reduceTaskId);
          for(File each : localIndex.listFiles()) { // /var/tmp/dmp/index/part_x
            if(each.isDirectory()) {
              for(File eachSub : each.listFiles()) { // /var/tmp/dmp/index/part_x/2015xxxx
                if(eachSub.isDirectory()) {
                  for(File indexFile : eachSub.listFiles()) { // /var/tmp/dmp/index/part_x/2015xxxx/*.*
                    if(indexFile.isFile()) {
                      System.out.println("delete exist file : " + indexFile.getAbsolutePath());
                      indexFile.delete();
                    }
                  }
                }
                System.out.println("delete exist date folder : " + eachSub.getAbsolutePath());
                eachSub.delete();
              }
            }
          }	
          if(!temp.exists()) {
            System.out.println("======== create job id file " + (localIndexPath + jobId) + " at task : " + reduceTaskId);
            temp.createNewFile();
          }
        } else {
          System.out.println("======== job id file " + (localIndexPath + jobId) + " existed no need to delete all " + " at task : " + reduceTaskId);
        }			
      } else {
        localIndex.mkdirs();
        if(!temp.exists()) {
          System.out.println("======== create job id file " + (localIndexPath + jobId) + " at task : " + reduceTaskId);
          temp.createNewFile();
        }
      }
    }catch(Exception ex) {
      System.out.println("Failed to delete file : " + ex.getMessage());
      ex.printStackTrace();
    }
    //wait for sec
    try {
      System.out.println("======== after delete all sleep 10s at task : " + reduceTaskId + " - " + System.currentTimeMillis());
      Thread.currentThread().sleep(10000);
    }catch(Exception ex) {
      ex.printStackTrace();
    }
    //create local partition index folder
    String pathName = localIndexPath + "part_" + reduceTaskId; ///var/tmp/dmp/index/part_x
    File eachLocalIndex = new File(pathName);
    if(eachLocalIndex.exists()) {
      if(eachLocalIndex.isDirectory()) {
        for(File dateFolder : eachLocalIndex.listFiles()) {
          /*
          if(dateFolder.isDirectory()) {
            for(File indexFile : dateFolder.listFiles()) {
              if(indexFile.isFile()) {
                System.out.println("delete exist file : " + indexFile.getAbsolutePath());
                indexFile.delete();
              }
            }
          }
          System.out.println("delete exist data folder : " + dateFolder.getAbsolutePath());
          dateFolder.delete();*/
          System.out.println("delete exist dir : " + dateFolder.toString());
          XdiReportDriver.recursiveDelete(dateFolder);
        }
      }
    } else {
      System.out.println("create folder : " + eachLocalIndex.getAbsolutePath() + " at task : "  + reduceTaskId);
      eachLocalIndex.mkdirs();
    }
    //create hdfs connection
    URI hdfsNamenode = null;
    try {
      System.out.println("======== create connection to " + hdfsNamenodeURL + " at task : " + reduceTaskId + " - " + System.currentTimeMillis());
      hdfsNamenode = new URI(hdfsNamenodeURL);		
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
    //copy remote index to local
    List<String> targetIndexList = null;
    if(hdfsNamenode != null) {
      FileSystem remoteFileSystem = null;
      try {
        //String remoteIndexPartPath = remoteIndexPath + "part_" + reduceTaskId;
        String remoteIndexPartPath = remoteIndexPath + "/archive";
        remoteFileSystem = FileSystem.get(hdfsNamenode, new Configuration());
        Path source = new Path(remoteIndexPartPath, "*");
        /*
				final FileStatus[] allFiles = remoteFileSystem.globStatus(source);
				for(FileStatus eachFile : allFiles) { 
					Path remotePath = eachFile.getPath();
					if(remotePath != null) {
						System.out.println("-- remote index file path : " + remotePath.toString());
						Path localPath = new Path(pathName);
						System.out.println(" ======> copy from HDFS: " + eachFile.getPath().toString() + " to local : " + localPath.toString() + " at task " + reduceTaskId);
						remoteFileSystem.copyToLocalFile(remotePath, localPath);
					} else {
						System.out.println("-- remote index file path is null");
					}
				}*/
        targetIndexList = listTargetIndex(remoteFileSystem, source, 1, sourceIndex);//due to running out disk space and RAM, change 5 to 1, hardcoded limit 5, 5 x 6 -> 30 days
        if(targetIndexList == null || targetIndexList.size() == 0)
          throw new Exception("Cannot find index " + sourceIndex + " from source " + source.toString() + " at task : "  + reduceTaskId);
        for(String eachData : targetIndexList) {
          Path remotePath = new Path(remoteIndexPartPath + "/" + eachData + "/" + curretDate + "/part_" + reduceTaskId, "*");
          System.out.println("-- remote index folder path : " + remotePath.toString());
          String strLocalPath = pathName + "/" + eachData;
          File local = new File(strLocalPath);
          if(!local.exists()) {
            System.out.println("create local path : " + strLocalPath);
            local.mkdirs();
          }
          Path localPath = new Path(strLocalPath);
          final FileStatus[] fs = remoteFileSystem.globStatus(remotePath);
          for(FileStatus f : fs) {
            if(f.isFile()) {
              System.out.println("copy remote file : " + f.getPath() + " to " + localPath);
              remoteFileSystem.copyToLocalFile(f.getPath(), localPath);   
            }
          }
        }
      }catch(Exception ex) {
        System.out.println("-- error when setup " + ex.getMessage());
        ex.printStackTrace();
      }
      //Create reader and searcher
      try {
        System.out.println("create search and reader at task " + reduceTaskId + " wait for 10 sec");
        Thread.currentThread().sleep(10000);
        analyzer = new WhitespaceAnalyzer();
        /*
				if(searcher == null) {
					synchronized(this) {
						if(searcher == null) {
							System.out.println("-- task["+reduceTaskId+"] open dir : " + pathName);
							try {
								Directory dir = NIOFSDirectory.open(new File(pathName));
								if(reader == null)
									reader = DirectoryReader.open(dir);
								if(searcher == null && reader != null)
									searcher = new IndexSearcher(reader);
							}catch(Exception e) {
								System.out.println("Failed to create reader and search for task : " + reduceTaskId + " retry in 10s");
								try {
									Thread.currentThread().sleep(10000);
									Directory dir = NIOFSDirectory.open(new File(pathName));
									if(reader == null)
										reader = DirectoryReader.open(dir);
									if(searcher == null && reader != null)
										searcher = new IndexSearcher(reader);
								}catch(Exception x) {
									System.out.println("Still failed to create reader and search for task : " + reduceTaskId + " " + x.getMessage());
								}
							}
						}
					}
				}
         */
        if(searchers == null) {
          synchronized(this) {
            if(searchers == null) {
              if(targetIndexList == null || targetIndexList.size() == 0)
                throw new Exception("Souce index " + sourceIndex + " failed to initialize at task : " + reduceTaskId);
              int indexSize = targetIndexList.size();
              System.out.println("prepare initial search and reader for size : " + indexSize + " at task : " + reduceTaskId);
              searchers = new IndexSearcher[indexSize];
              readers = new IndexReader[indexSize];
              for(int i = 0; i < indexSize; i++) {
                String localTarget = pathName + "/" + targetIndexList.get(i);
                File localTargetPath = new File(localTarget);
                if(!localTargetPath.exists())
                  throw new Exception("Cannot find local index path " + localTarget + " at task : " + reduceTaskId);
                System.out.println("-- task["+reduceTaskId+"] open dir : " + localTarget + " at index : " + i);
                try {
                  Directory dir = NIOFSDirectory.open(new File(localTarget));
                  if(readers[i] == null)
                    readers[i] = DirectoryReader.open(dir);
                  if(searchers[i] == null && readers[i] != null)
                    searchers[i] = new IndexSearcher(readers[i]);
                }catch(Exception e) {
                  System.out.println("Failed to create reader and search for task : " + reduceTaskId + " retry in 10s");
                  try {
                    Thread.currentThread().sleep(10000);
                    Directory dir = NIOFSDirectory.open(new File(localTarget));
                    if(readers[i] == null)
                      readers[i] = DirectoryReader.open(dir);
                    if(searchers[i] == null)
                      searchers[i] = new IndexSearcher(readers[i]);
                  }catch(Exception x) {
                    System.out.println("Still failed to create reader and search at index " + i + " for task : " + reduceTaskId + " " + x.getMessage());
                  }
                }
              }
            }
          }
        }
      }catch(Exception ex) {
        System.out.println("-- error when initial " + ex.getMessage());
        ex.printStackTrace();
      }
    }
  }

  /**
   * 
   * @param remoteFileSystem : HDFS
   * @param source : /user/dmp/xdi/index/part_x
   * @param limits : retrieve index folders size
   * @param sourceIndex : (20150927) inside /user/dmp/xdi/result/20150927/screen6
   * @return List of String : {20150922, 20150928, 20151004 ...}
   * @throws Exception
   */
  private static List<String> listTargetIndex(FileSystem remoteFileSystem, Path source, int limits, String sourceIndex) throws Exception{
    final FileStatus[] allFiles = remoteFileSystem.globStatus(source);
    if(allFiles != null && allFiles.length > 0) {
      int[] dates = new int[allFiles.length];
      int index = 0;
      for(FileStatus eachDate : allFiles) { 
        String each = eachDate.getPath().getName();
        dates[index] = Integer.parseInt(each);
        index++;
      }
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
      return res;
    } else {
      throw new Exception("Empty file path");
    }
  }

  /**
   * Key : value
   * value : key&vendor
   */
  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    int reduceTaskId = context.getTaskAttemptID().getTaskID().getId();
    try {
      int sSize = searchers.length;
      String res = null;
      long start = System.currentTimeMillis();
      for(int s = 0; s < sSize; s++) {
        IndexSearcher searcher = searchers[s];
        if(searcher == null) {
          System.out.println("searchers not correctly initialized in task : " + reduceTaskId);
          return;
          //throw new InterruptedException("searchers not correctly initialized in task : " + reduceTaskId);
        }
        TopScoreDocCollector collector = TopScoreDocCollector.create(1, true);
        String searchText = key.toString();
        Query q = new QueryParser("storageid", analyzer).parse(searchText);
        //System.out.println("Query : " + searchText + " at task : " + reduceTaskId);
        searcher.search(q, collector);
        ScoreDoc[] hits = collector.topDocs().scoreDocs;
        if(hits == null || hits.length == 0) {
          //System.out.println("Cannot find result for query " + searchText + " in searchers["+s+"], move to next at task : " + reduceTaskId);
          continue;
        } else {
          long cost =  System.currentTimeMillis() - start;
          for(int i = 0; i < hits.length; i++) {
            int docId = hits[i].doc;
            Document d = searcher.doc(docId);
            res = CompressionTools.decompressString(d.getBinaryValue("mapinfo"));
            //System.out.println("Find query : " + searchText + " res : " + res + " takes : " + cost + " at task : " + reduceTaskId);
          }
          break;
        }
      }		
      //if(res != null && res.length() > 0) {
      for(Text value : values) {
        String[] keyAndVendor = value.toString().split("&");
        if(keyAndVendor.length > 1) {
          String keyText = keyAndVendor[0];
          resKey.set(keyText);
          String vendorText = keyAndVendor[1];
          if(res != null && res.length() > 0) {
            String valueText = res + "&" + vendorText;
            result.set(valueText);
            mos.write("result", resKey, result, resultOutput);
            //System.out.println("-> " + keyText + " - " + valueText + " at task : " + reduceTaskId);
            //context.write(resKey, result);
          } else {
            result.set(key.toString() + "&" + vendorText);
            //mos.write("remain", resKey, key, remainOutput); [Henry] value -> key + vendor
            mos.write("remain", resKey, result, remainOutput);
          }
        }
      }
      //}
    }catch(Exception ex) {
      System.out.println("-> error " + ex.getMessage() + " at task : " + reduceTaskId);
      ex.printStackTrace();
    }
  }

  @Override
  protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException, InterruptedException {
    try {
      int reduceTaskId = context.getTaskAttemptID().getTaskID().getId();
      if(readers != null) {
        int index = 0;
        for(IndexReader reader : readers) {
          System.out.println("==== cleanup searchers["+index+"] shutdown reader at " + reduceTaskId);
          reader.close();
          index++;
        }
      }
      //remove previous temp job files, not current one.
      String jobId = context.getJobID().toString();
      File jobTemp = new File(localIndexPath);
      if(jobTemp.exists() && jobTemp.isDirectory()) {
        for(File each : jobTemp.listFiles()) {
          if(each.isFile()) {
            String fileName = each.getName();
            StringBuffer buffer = new StringBuffer();
            buffer.append(fileName);
            if(!fileName.equals(jobId)) {
              each.delete();
              buffer.append("-");
              buffer.append("deleted");
            } else {
              buffer.append("-");
              buffer.append("keep");
            }
            System.out.println(" each job file " + buffer.toString() + " at task : " + reduceTaskId);
          }
        }
      }
      System.out.println("==== cleanup finished at " + reduceTaskId);
    }catch(Exception ex) {
      ex.printStackTrace();
    } finally {
      if(mos != null)
        mos.close();
    }
  }
}
