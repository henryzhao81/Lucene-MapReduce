package com.openx.audience.xdi.lucene;

public class Tuple {
  String key;
  String value;

  public Tuple(String key, String value) {
    this.key = key;
    this.value = value;
  }

  public String getKey() {
    return key;
  }
  public String getValue() {
    return value;
  }
}
