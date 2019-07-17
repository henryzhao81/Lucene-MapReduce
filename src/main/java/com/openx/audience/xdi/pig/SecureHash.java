package com.openx.audience.xdi.pig;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * This Pig UDF takes a string as input and returns a MD5
 *
 * @author larry.lai
 * @created 10/1/15.
 */
public class SecureHash extends EvalFunc<String> {
  @Override
  public String exec(Tuple tuple) {
    String msg = null;
    try {
      if (tuple == null || tuple.size() == 0 || tuple.get(0) == null) {
        return null;
      }
      msg = (String) tuple.get(0);
      if (msg != null && !msg.isEmpty())
        msg = new URL(msg).getHost();
    } catch (MalformedURLException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace(); 
    }
    return DigestUtils.md5Hex(msg);
  }
}
