package com.openx.audience.xdi.lucene;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class LoadGroupHandler {

  FileSystem remoteFileSystem = null; 
  int loadNum = -1; 
  String localPath = null;
  LoadHandler[] handlers;
  
  public LoadGroupHandler(FileSystem remoteFileSystem, int loadNum, String localPath) {
    handlers = new LoadHandler[loadNum];
    for(int i = 0; i < loadNum; i++) {
      handlers[i] =  new LoadHandler(remoteFileSystem, this, localPath, i);
    }  
  }
  
  private ConcurrentLinkedQueue<Path> queue = new ConcurrentLinkedQueue<Path>();
  
  public void execute() {
    if(handlers != null && handlers.length > 0) {
      CountDownLatch latch = new CountDownLatch(handlers.length);
      for(LoadHandler handler : handlers) {
        handler.setLatch(latch);
        handler.start();
      }
      try {
        latch.await();
      }catch(InterruptedException ex) {
        ex.printStackTrace();
      }
    }
  }
  
  public void addPath(Path path) {
    this.queue.offer(path);
  }

  public Path getPath() {
    return this.queue.poll();
  }
}
