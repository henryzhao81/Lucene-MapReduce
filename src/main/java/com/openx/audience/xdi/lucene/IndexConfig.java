package com.openx.audience.xdi.lucene;

import java.io.InputStream;
import java.util.Properties;

public class IndexConfig {

  private static IndexConfig config = new IndexConfig();
  private Properties properties;
  
  private IndexConfig() {
    this.load();
  }
  
  public static IndexConfig getInstance() {
    return config;
  }
  
  private void load() {
    try {
      properties = new Properties();
      String propFile = "config.properties";
      InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFile);
      if(inputStream != null) {
        properties.load(inputStream);
      } else {
        throw new Exception("Failed to load property file");
      }
    }catch(Exception ex) {
      ex.printStackTrace();
    }
  }
  
  public int getPartitionSize() {
    return Integer.parseInt(properties.getProperty("PARTITION_NUM"));
  }
  
  public int getSlotSize() {
    return Integer.parseInt(properties.getProperty("SLOT_SIZE"));
  }
}
