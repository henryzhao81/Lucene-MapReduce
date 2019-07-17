package com.openx.audience.xdi.lucene;

import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.io.Text;

public class BuilderGroupHandler {

  BuilderHandler[] handlers = null;

  public BuilderGroupHandler(int partition, String output) {
    this.init(partition, output);
  }

  private void init(int partition, String output) {
    handlers = new BuilderHandler[partition];
    for(int i = 0; i < partition; i++) {
      handlers[i] = new BuilderHandler(i, output);			
    }
  }

  public void startGroup() {
    if(handlers != null && handlers.length > 0) {
      for(BuilderHandler handler : handlers) {
        handler.start();
      }
    }
  }

  public BuilderHandler[] getHandlers() {
    return handlers;
  }

  public void addTuple(Tuple tuple) {
    int index = this.getPartition(tuple);
    if(index > -1)
      handlers[index].addMessage(tuple);
  }

  public int getPartition(Tuple tuple) {
    int index = -1;
    if(handlers != null && handlers.length > 0) {
      int size = handlers.length;
      //index = Math.abs(tuple.getKey().hashCode()) % size;
      Integer in = tuple.getKey().hashCode();
      index = (int)(Math.abs(in.longValue()) % size);
    }
    return index;
  }

  public int getPartition(Text key, Text value, int numPartitions) {
    Integer in = key.toString().hashCode();
    int id = (int)(Math.abs(in.longValue()) % numPartitions);
    return id;
  }

  public void shutdown(boolean needMerge) throws InterruptedException {
    if(handlers != null && handlers.length > 0) {
      CountDownLatch latch = new CountDownLatch(handlers.length);
      for(BuilderHandler handler : handlers) {
        handler.shutdown(latch, needMerge);
      }
      latch.await();
    }
  }
}
