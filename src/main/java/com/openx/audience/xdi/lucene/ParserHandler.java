package com.openx.audience.xdi.lucene;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class ParserHandler extends Thread {
  BuilderGroupHandler buildHandler;
  ParserGroupHandler parserHandler;
  String localPath;
  FileSystem remoteFileSystem;
  int index;
  Path remotePath;
  boolean isRunning;
  CountDownLatch latch;
  //int curretFileIndex = 0;

  private final static Map<String, Integer> weightMap;
  static {
    weightMap = new HashMap<String, Integer>();
    weightMap.put("Internet Explorer", 100);
    weightMap.put("Chrome", 80);
    weightMap.put("Firefox", 60);
    weightMap.put("Other", 40);
  }

  public ParserHandler(BuilderGroupHandler buildHandler, ParserGroupHandler parserHandler, String localPath, int index, FileSystem remoteFileSystem) {
    this.buildHandler = buildHandler;
    this.parserHandler = parserHandler;
    this.localPath = localPath;
    this.index = index;
    this.remoteFileSystem = remoteFileSystem;
  }

  private Path copyToLocal() throws InterruptedException, IOException  {
    //		remotePath = parserHandler.getPath();
    //		if(remotePath != null) {
    //			FileSystem remoteFileSystem = FileSystem.get(hdfsNamenode, new Configuration());
    //			final FileStatus[] allFiles = remoteFileSystem.globStatus(remotePath);
    //			for(FileStatus eachFile : allFiles) {
    //				if(eachFile.isFile()) {
    //					Path destPath = new Path(this.localPath);
    //					System.out.println(" ======> copy from HDFS: " + eachFile.getPath().toString() + " to local : " + destPath.toString() + " at task " + this.index);
    //					remoteFileSystem.copyToLocalFile(eachFile.getPath(), destPath);
    //					return destPath;
    //				}
    //			}
    //		}
    //		return null;
    remotePath = parserHandler.getPath();
    //System.out.println("current file index : " + (this.curretFileIndex++));
    return remotePath;
  }

  //Key : NullWritable / BytesWritable  Value : Text
  public void setLatch(CountDownLatch latch) {
    this.latch = latch;
  }

  private String getLocalPath() {
    //    	if(localPath != null && remotePath != null) {
    //    		String fileName = remotePath.getName();
    //    		return localPath + "/" + fileName;
    //    	} else 
    //    		return null;
    return this.remotePath.toString();
  }

  private void readFile() throws Exception {
    String fileName = this.getLocalPath();
    System.out.println("read from local path : " + fileName + " at task: " + index);
    Configuration conf = new Configuration();
    Path path = new Path(fileName);
    SequenceFile.Reader reader = null;
    try {
      reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
      Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
      Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
      while(reader.next(key, value)) {
        //String[] words = value.toString().split("\t", 18);
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
            String strTime = words[0].trim();
            Date date = sdf.parse(strTime);
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
          //    				if(!this.vset.contains(vid)) {
          //        				Tuple tuple = new Tuple(vid, mid+"&"+browserType);
          //    					this.vset.add(vid);
          //    				    this.buildHandler.addTuple(tuple);
          //    				}
          if(!this.parserHandler.containsInMap(vid)) {
            Tuple tuple = new Tuple(vid, mid+"&"+browserType);
            this.parserHandler.addToMap(vid);
            this.buildHandler.addTuple(tuple);
          }
        }
      }
    }catch(Exception ex) {
      throw new Exception("Failed to read file : " + this.getLocalPath() + " - " + ex.getMessage());
    }finally{
      IOUtils.closeStream(reader);
    }
  }

  //5  ->  3
  //8  ->  4
  //15 ->  14 
  /* READ FOR GZIP
    private void readFile() throws Exception{
    	String fileName = this.getLocalPath();
    	System.out.println("read from local path : " + fileName + " at task: " + index);
    	FileInputStream fis = null;
		GZIPInputStream gis = null;
		try {
			fis = new FileInputStream(fileName);
			gis = new GZIPInputStream(fis);
			Reader decoder = new InputStreamReader(gis);
			BufferedReader br = new BufferedReader(decoder);
			String line;
			long pos = 0;
			while ((line = br.readLine()) != null) {
				//System.out.println("==========="+(pos++)+"=========== at task: " + index);
				try {
					String[] words = line.split("\u0001");
					if(words.length > 3) {
						String key = null;
						if(words.length >= 5 && words[5] != null && words[5].length() > 0)
							key = words[5];
						String info = null;
						if(words.length >= 15) {
							if(words[8] != null && words[8].length() > 0)
                                info = words[8];
							if(info != null && words[15] != null && words[15].length() > 0) {
								String browserName = words[15];
								if(!browserName.contains("Safari"))
								    info = info + "$" + browserName;
								else
									info = null;
							}
						}
						if(key != null && info != null) {
							Tuple tuple = new Tuple(key, info);
							this.buildHandler.addTuple(tuple);
						}
					}
				}catch(Exception ex) {
					System.out.println("Failed to process line : " + line);
                    ex.printStackTrace();
				}
			}
			System.out.println("finish read from local path : " + fileName + " at task: " + index);
		}catch(Exception ex) {
			throw new Exception("Failed to read file : " + this.localPath + " - " + ex.getMessage());
		}finally{
			try {
				if(gis != null)
					gis.close();
				if(fis != null) {
					fis.close();
				}
			}catch(Exception ex) {
				ex.printStackTrace();
			}
		}
    }*/

  @Override
  public void run() {
    while(true) {
      Path dest = null;
      try {
        dest = this.copyToLocal();
      }catch(Exception ex) {
        System.out.println("Failed to copy file from hdfs: " + this.remotePath.toString() +" to local: " + this.localPath + " at task " + this.index);
        ex.printStackTrace();
        break;
      }
      if(dest == null) {
        System.out.println("No Path object found in queue, exit" + " at task " + this.index);
        break;
      }
      System.out.println("copy successfully at task " + this.index);
      try {
        this.readFile();
      } catch(Exception ex) {
        ex.printStackTrace();
      }
      if(this.getLocalPath() != null) {
        try {
          System.out.println("delete file : " + this.getLocalPath() + " at task " + this.index);
          File file = new File(this.getLocalPath());
          boolean successful = file.delete();
          if(!successful)
            System.out.println("file to delete file : " + this.getLocalPath() + " at task " + this.index);
        }catch(Exception ex) {
          System.out.println("file to delete file : " + this.getLocalPath() + " at task " + this.index);
          ex.printStackTrace();
        }
      }
      System.out.println("get next file at task " + this.index);
    }
    System.out.println("Finish retrieve remote files at task : " + this.index + " ready to shutdown parser thread");
    if(latch != null)
      latch.countDown();
  }
}
