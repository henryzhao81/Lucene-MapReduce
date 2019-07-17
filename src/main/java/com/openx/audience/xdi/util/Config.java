package com.openx.audience.xdi.util;

import java.io.InputStream;
import java.util.Properties;

public class Config {

  private static Config config = new Config();
  private Properties properties;

  private Config() {
    this.load();
  }
  
  public static Config getInstance() {
    return config;
  }
  
  private void load() {
    try {
      properties = new Properties();
      String proFile = "config.properties";
      InputStream inputStream = getClass().getClassLoader().getResourceAsStream(proFile);
      if(inputStream != null) {
        properties.load(inputStream);
      } else {
        throw new Exception("Failed to load property file");
      }
    }catch(Exception ex) {
      ex.printStackTrace();
    }
  }
  
  public String getPropValue(String key) {
    return properties.getProperty(key);
  }
  
  public String getStatusPath() {
    return properties.getProperty("ALDMF_STATUS_PATH");
  }
  
  public String getAudHdfsUrl() {
    return properties.getProperty("ALDMF_AUD_HDFSURL");
  }
  
  public String getAudPath() {
    return properties.getProperty("ALDMF_AUD_PATH");
  }
  
  public String getAudTsvPath() {
    return properties.getProperty("ALDMF_AUD_TSV");
  }
  
  public String getAudHFilePath() {
    return properties.getProperty("ALDMF_AUD_HFILE");
  }
  
  public String getGridHdfsUrl() {
    return properties.getProperty("ALDMF_GRID_HDFSURL");
  }
  
  public String getGridDeltaPath() {
    return properties.getProperty("ALDMF_GRID_DELTA_PATH");
  }
  
  public String getGridFullPath() {
    return properties.getProperty("ALDMF_GRID_FULL_PATH");
  }
  
  public String getLockURL() {
    return properties.getProperty("ALDMF_GRID_LOCK_URL");
  }
  
  public String getLockName() {
    return properties.getProperty("ALDMF_GRID_LOCK_NAME");
  }
}
