package com.openx.audience.xdi.lucene;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ParserGroupHandler {

  ParserHandler[] handlers;
  BuilderGroupHandler buildHandler;

  private Map<String, Integer> pMap;

  public ParserGroupHandler(BuilderGroupHandler handler, int parserNum, String localPath, FileSystem remoteFileSystem) {
    this.pMap = new ConcurrentHashMap<String, Integer>(); 
    this.buildHandler = handler;
    this.init(handler, parserNum, localPath, remoteFileSystem);
  }

  private void init(BuilderGroupHandler handler, int parserNum, String localPath, FileSystem remoteFileSystem) {
    handlers = new ParserHandler[parserNum];
    for(int i = 0; i < parserNum; i++) {
      handlers[i] = new ParserHandler(handler, this, localPath, i, remoteFileSystem);
    }
  }

  private ConcurrentLinkedQueue<Path> queue = new ConcurrentLinkedQueue<Path>();

  public void execute() {
    if(handlers != null && handlers.length > 0) {
      CountDownLatch latch = new CountDownLatch(handlers.length);
      for(ParserHandler handler : handlers) {
        handler.setLatch(latch);
        handler.start();
      }
      try {
        latch.await();
      }catch(InterruptedException ex) {
        ex.printStackTrace();
      }
      /* TODO move to IndexBuilder
			System.out.println("send shutdwon to all build handler");
			try {
				this.buildHandler.shutdown();
			}catch(InterruptedException ex) {
				System.out.println("Error when shutdown build handler thread");
                ex.printStackTrace();
			}
			System.out.println("shutdown completely");
       */
    }
  }

  public void addToMap(String key) {
    pMap.put(key, 1);
  }

  public boolean containsInMap(String key) {
    return pMap.containsKey(key);
  }

  public void addPath(Path path) {
    this.queue.offer(path);
  }

  public Path getPath() {
    return this.queue.poll();
  }
}
