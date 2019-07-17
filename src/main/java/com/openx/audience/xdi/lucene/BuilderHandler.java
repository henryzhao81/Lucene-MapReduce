package com.openx.audience.xdi.lucene;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.CompressionTools;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Version;

public class BuilderHandler extends Thread {
  int partNumber;
  CountDownLatch shutdownLatch;
  IndexWriter writer;
  boolean running = true;
  private static final String KEY_STORAGE_ID = "storageid";
  private static final String KEY_MAP_INFO = "mapinfo";

  private static final String hdfsNamenodeURL = "hdfs://xvaic-sg-36.xv.dc.openx.org:8020";//PROD
  //private static final String hdfsNamenodeURL = "hdfs://xvaa-i35.xv.dc.openx.org:8020";//QA
  private static final String archieve = "/user/dmp/xdi/index/archive";
  private static final String localStage = "/var/tmp/stage/";

  private String outputPath; // /var/tmp/stage/2015/10/07/18

  public BuilderHandler(int partNumber, String output) {
    this.partNumber = partNumber;
    this.outputPath = output;
    this.initialIndex(output);
  }

  /**
   * @param output /var/tmp/stage/2015/10/07/18
   */
  public void initialIndex(String output) {
    try {
      String targetStageIndexPath = this.copyTargetIndexToLocal(output);
      this.writer = this.createIndex(targetStageIndexPath);
    }catch(Exception ex) {
      System.out.println("Failed to create index part_"+partNumber);
      ex.printStackTrace();
    }
  }
  
  public BuilderHandler() {}

  /**
   * @param output /var/tmp/stage/2015/10/07/18
   */
  private String copyTargetIndexToLocal(String output) {
    String result = null;
    try {
      URI hdfsNamenode = null;
      try {
        System.out.println("======== create connection to " + hdfsNamenodeURL + " at task : " + partNumber + " - " + System.currentTimeMillis());
        hdfsNamenode = new URI(hdfsNamenodeURL);		
      } catch (URISyntaxException e) {
        e.printStackTrace();
      }
      if(hdfsNamenode != null) {
        FileSystem remoteFileSystem = null;
        try {
          Configuration hadoopConfig = new Configuration();
          hadoopConfig.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
          hadoopConfig.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
          remoteFileSystem = FileSystem.get(hdfsNamenode, hadoopConfig);
          Path source = new Path(archieve, "*"); 
          String sourceIndex = this.getSourceIndex(output);
          result = localStage + sourceIndex + "/part_" + partNumber; // /var/tmp/stage/20151007/part_x					
          File localIndexFile = new File(result);
          if(!localIndexFile.exists()) {
            System.out.println("looking for target index : " + sourceIndex);
            List<String> targetIndexList = this.listTargetIndex(remoteFileSystem, source, 1, sourceIndex);
            if(targetIndexList == null || targetIndexList.size() == 0) {
              System.out.println("Cannot find index " + sourceIndex + " from source " + source.toString() + " at task : "  + partNumber);
            } else {
              /*
              Path partSource = new Path(archieve + "/" + targetIndexList.get(0) + "/part_" + partNumber + "/", "*");
              System.out.println("try to get partition : " + partSource.toString());
              final FileStatus[] allFiles = remoteFileSystem.globStatus(partSource);
              List<Path> paths = new ArrayList<Path>();
              for(FileStatus eachDate : allFiles) {
                if(eachDate.isDirectory()) {
                  Path eachPartPath = eachDate.getPath();
                  paths.add(eachPartPath);
                }
              }
              Path previous = this.getPrevious(paths, sourceIndex);
              if(previous != null) {
                Path localStagePath = new Path(result);
                System.out.println("copy remote partition index : " + previous.toString() + " to local path : " + localStagePath.toString());
                remoteFileSystem.copyToLocalFile(previous, localStagePath);
              }
              */
              Path partSource = new Path(archieve + "/" + targetIndexList.get(0) + "/", "*");
              System.out.println("try to get partition : " + partSource.toString());
              final FileStatus[] allFiles = remoteFileSystem.globStatus(partSource);
              List<Path> paths = new ArrayList<Path>();
              for(FileStatus eachDate : allFiles) {
                if(eachDate.isDirectory()) {
                  Path eachPartPath = eachDate.getPath();
                  paths.add(eachPartPath);
                }
              }
              Path previous = this.getPrevious(paths, sourceIndex, partNumber);
              if(previous != null) {
                Path localStagePath = new Path(result);
                System.out.println("copy remote partition index : " + previous.toString() + " to local path : " + localStagePath.toString());
                remoteFileSystem.copyToLocalFile(previous, localStagePath);
              }
            }
          } else {
            System.out.println("local index file " + result + " exist !");
          }
        }catch(Exception ex) {
          System.out.println("-- error when setup " + ex.getMessage());
          ex.printStackTrace();
        }
//        finally {
//          if(remoteFileSystem != null)
//            remoteFileSystem.close();
//        }
      }		
    }catch(Exception ex) {
      ex.printStackTrace();
    }
    return result;
  }

  private Path getPrevious(List<Path> paths, String target, int partNumber) {
    Collections.sort(paths, new Comparator<Path>() {
      @Override
      public int compare(Path p1, Path p2) {
        return Integer.parseInt(p1.getName()) - Integer.parseInt(p2.getName());
      }
    });
    if(paths != null && paths.size() > 0) {
      Path latest = paths.get(paths.size() - 1);
      if(Integer.parseInt(latest.getName()) < Integer.parseInt(target)) {
        System.out.println("find previous path " + (latest.toString() + "/part_" + partNumber));
        return new Path(latest.toString() + "/part_" + partNumber);
      }
    }
    return null;
  }

  /**
   * @param source /var/tmp/stage/2015/10/07/18
   * @return eg. 20151007
   */
  private String getSourceIndex(String source) {
    String output = source.substring(source.indexOf(localStage) + localStage.length());
    String[] split = output.split("/");
    //[2015, 10, 06]
    return split[0] + split[1] + split[2];
  }

  private List<String> listTargetIndex(FileSystem remoteFileSystem, Path source, int limits, String sourceIndex) throws Exception{
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
      if(i >= dates.length || Integer.parseInt(sourceIndex) > dates[i]) {
        int last = dates[dates.length - 1];
        System.out.println("Index for current date have not been generated yet and last is : " + last);
        int newData = this.createTargetIndex(remoteFileSystem, last, sourceIndex);
        if(newData > 0) {
          queue.clear();
          queue.offer(newData);
        }
      }
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

  private static final int SLOT_SIZE = IndexConfig.getInstance().getSlotSize();
  private int createTargetIndex(FileSystem remoteFileSystem, int last, String target) {
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
      c.add(Calendar.DATE, SLOT_SIZE);
      tempTime = sdf.format(c.getTime());
      String[] newDataSlot = tempTime.split("-");
      String newData = newDataSlot[0] + newDataSlot[1] + newDataSlot[2];
      /*//TODO : no restrict
      if(Integer.parseInt(newData) >= Integer.parseInt(target)) {
        Path result = new Path(archieve + "/" + newData);
        System.out.println("create remote date slot : " + result.toString());
        remoteFileSystem.mkdirs(result);
        return Integer.parseInt(newData);
      } else {
        throw new Exception("target date is over 6 days");
      }*/
      Path result = new Path(archieve + "/" + newData);
      System.out.println("create remote date slot : " + result.toString());
      remoteFileSystem.mkdirs(result);
      return Integer.parseInt(newData);
    }catch(Exception ex) {
      System.out.println("Failed to create new date slot "  + ex.getMessage());
      ex.printStackTrace();
    }
    return -1;
  }

  private void shutdownAndOptimize(boolean needMerge) {
    System.out.println("optimize and close index for part : " + this.partNumber);
    long startTime = System.currentTimeMillis();
    try {
      if(writer != null) {
        while(this.queue.size() > 0) {
          System.out.println("queue is not empty, try to close after 5s at part : " + this.partNumber);
          Thread.currentThread().sleep(5000);
        }
        if(needMerge) {
          System.out.println("merge index writer for part : " + this.partNumber);
          writer.forceMerge(1);
        }
        System.out.println("commit index writer for part : " + this.partNumber);
        writer.commit();
        System.out.println("close index writer for part : " + this.partNumber);
        writer.close();
      }
    }catch(Exception ex) {
      System.out.println("Failed to close index writer at part : " + this.partNumber);
      ex.printStackTrace();
    }
    //create tag file
    System.out.println("create temp tag file for take " +  this.partNumber);
    if(outputPath != null) {
      try {
        String res = outputPath.substring(outputPath.indexOf(localStage) + localStage.length());
        String[] split = res.split("/"); //[2015, 10, 06, 00]
        String sourceIndex = this.getSourceIndex(outputPath);
        File tempFile = new File(localStage + sourceIndex + "/DONE_part_" + this.partNumber+"_"+split[0]+split[1]+split[2]+split[3]);
        if(!tempFile.exists())
          tempFile.createNewFile();
      }catch(Exception ex) {
        ex.printStackTrace();
      }
    }
    System.out.println("optimize and close index for part : " + this.partNumber + " finished, takes : " + (System.currentTimeMillis() - startTime) / 1000 + " s.");
  }

  public void shutdown(CountDownLatch shutdownLatch, boolean needMerge) {
    System.out.println("prepare to optimize and shutdown index writer part : " + this.partNumber);
    this.shutdownLatch = shutdownLatch;
    /*try {
			if(writer != null) {
				while(this.queue.size() > 0) {
					System.out.println("queue is not empty, try to close after 5s at part : " + this.partNumber);
					Thread.currentThread().sleep(5000);
				}
				System.out.println("close index writer for part : " + this.partNumber);
				//writer.close();
				writer.forceMerge(1);
				writer.commit();
			}
		}catch(Exception ex) {
			System.out.println("Failed to close index writer at part : " + this.partNumber);
            ex.printStackTrace();
		}
		//create tag file
		System.out.println("create temp tag file for take " +  this.partNumber);
		if(outputPath != null) {
			try {
				String res = outputPath.substring(outputPath.indexOf(localStage) + localStage.length());
				String[] split = res.split("/"); //[2015, 10, 06, 00]
				String sourceIndex = this.getSourceIndex(outputPath);
				File tempFile = new File(localStage + sourceIndex + "/DONE_part_" + this.partNumber+"_"+split[0]+split[1]+split[2]+split[3]);
				if(!tempFile.exists())
					tempFile.createNewFile();
			}catch(Exception ex) {
                ex.printStackTrace();
			}
		}*/
    try {
      while(this.queue.size() > 0) {
        System.out.println("queue is not empty, try to shutdown after 5s at part : " + this.partNumber);
        Thread.currentThread().sleep(5000);
      }
      //shutdownAndOptimize(needMerge);
      //this.running = false;
      if(needMerge) {
        this.queue.add(new Tuple("shutdown_merge", "."));
      }else {
        this.queue.add(new Tuple("shutdown", "."));
      }
    }catch(Exception ex) {
      System.out.println("Failed to notify blocking queue at part : " + this.partNumber);
      ex.printStackTrace();
    }
  }

  private BlockingQueue<Tuple> queue = new ArrayBlockingQueue<Tuple>(5000000);
  public void addMessage(Tuple tuple) {
    try {
      queue.put(tuple);
    }catch(Exception ex){
      ex.printStackTrace();
    }
  }

  private Tuple getMessage() {
    Tuple tuple = null;
    try {
      tuple = queue.take();
    }catch(Exception ex) {
      ex.printStackTrace();
    }
    return tuple;
  }

  private IndexWriter createIndex(String output) throws IOException, ParseException {
    //String pathname = "/Users/hzhao/Documents/workspace/luceneTest/index/screen6.part_"+partNumber;//"/home/hzhao/hzhao/luceneTest/output/screen6.part_"+partNumber;//
    //String pathName = output+"_"+this.partNumber;
    System.out.println("======== setup FSDirectory open path : " + output + " at " + this.partNumber);
    Directory dir = NIOFSDirectory.open(new File(output));
    final IndexWriter indexWriter = this.createIndexWriter(dir);
    return indexWriter;
  }

  private IndexWriter createIndexWriter(Directory directory) throws IOException {
    //StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_47);
    Analyzer analyzer = new WhitespaceAnalyzer();
    IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_4_10_3, analyzer);//TODO upgrade to LUCENE_4_10_4
    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
    return new IndexWriter(directory, iwc);
  }

  @Override
  public void run() {
    long tupleNum = 0;
    System.out.println("Builder Handler is ready to buid index at part : " + this.partNumber);
    while(running) {
      Tuple tuple = this.getMessage();
      if(!tuple.key.contains("shutdown")) {
        try {
          //System.out.println("Build index : " + tuple.getKey() + " at part : " + this.partNumber);
          this.insertIndex(tuple);
          tupleNum++;
        }catch(Exception ex) {
          System.out.println("Failed to build tuple key : " + tuple.getKey() + " at part : " + this.partNumber + " --- " + ex.getMessage());
        }
      } else {
        this.running = false;
        String tupleKey = tuple.key;
        if(tupleKey.contains("merge")) {
          this.shutdownAndOptimize(true);
        } else {
          this.shutdownAndOptimize(false);
        }
      }
    }
    System.out.println("ready to shutdown thread for part : " + this.partNumber + " tuple number : " + tupleNum);
    if(this.shutdownLatch != null)
      this.shutdownLatch.countDown();
    System.out.println("shutdown thread for part : " + this.partNumber);
  }

  private void insertIndex(Tuple tuple) throws Exception {
    if(tuple != null) {
      Document doc = new Document();
      doc.add(new StringField(KEY_STORAGE_ID, tuple.getKey(), Field.Store.YES));
      //doc.add(new TextField(KEY_MAP_INFO, tuple.getValue(), Field.Store.YES));
      doc.add(new StoredField(KEY_MAP_INFO, CompressionTools.compress(tuple.getValue().getBytes())));
      this.writer.updateDocument(new Term(KEY_STORAGE_ID, tuple.getKey()), doc);
      //this.writer.forceMergeDeletes();
      //this.writer.addDocument(doc);
    }
  }
}
