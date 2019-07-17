package com.openx.audience.xdi.lucene;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class LoadHandler extends Thread {

  CountDownLatch latch;
  FileSystem remoteFileSystem;
  LoadGroupHandler gHandler;
  String localPath;
  int index;
  
  public LoadHandler(FileSystem remoteFileSystem, LoadGroupHandler gHandler, String localPath, int index) {
    this.remoteFileSystem = remoteFileSystem;
    this.gHandler = gHandler;
    this.localPath = localPath;
    this.index = index;
  }
  
  private Path copyToLocal() throws InterruptedException, IOException  {
    Path remoteFilePath = gHandler.getPath();
    if(remoteFilePath != null) {
      System.out.println("copy " + remoteFilePath.toString() + " to local " + this.localPath + " at task : " + index);
      remoteFileSystem.copyToLocalFile(remoteFilePath, new Path(localPath + "/" + remoteFilePath.getName()));
    }
    return remoteFilePath;
  }
  
  public void setLatch(CountDownLatch latch) {
    this.latch = latch;
  }
  
  @Override
  public void run() {
    while(true) {
      Path dest = null;
      try {
        dest = this.copyToLocal();
      }catch(Exception ex) {
        System.out.println("Failed to copy file from hdfs");
        ex.printStackTrace();
        break;
      }
      if(dest == null) {
        System.out.println("No Path object found in queue, exit" + " at task " + this.index);
        break;
      }
    }
    System.out.println("Finish retrieve remote files at task : " + this.index + " ready to shutdown load thread");
    if(latch != null)
      latch.countDown();
  }
}
