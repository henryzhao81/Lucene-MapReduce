package com.openx.audience.xdi.pig;

import com.openx.audience.xdi.pig.SecureHash;
//import com.openx.audience.xdi.pig.SequenceFileLoader;




import static org.junit.Assert.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.*;

import java.io.IOException;

public class SecureHashTest {

  private Tuple tuple;
  protected static final Log LOG = LogFactory.getLog(SecureHash.class);
  
  @Before
  public void setup() throws Exception {
    tuple = TupleFactory.getInstance().newTuple();
  }

  @Test
  public void testSecureHash() throws IOException {
    SecureHash hash = new SecureHash();

    tuple.append("http://www.macleans.ca/schools/nipissing-university/");
    LOG.info("The current url is " + tuple.get(0));
    
    String domain_hashcode = hash.exec(tuple);
    LOG.info("The hashcode is " + domain_hashcode);
    
    tuple.set(0, "http://www.macleans.ca/news/canada/politweet/");
    LOG.info("Now the current url is " + tuple.get(0));
    
    String host_hashcode = hash.exec(tuple);
    LOG.info("Now the hashcode is " + host_hashcode);
    assertEquals(host_hashcode, domain_hashcode);
  }

  @Test
  public void testSecureHashTupleIndexNull() throws IOException {
    SecureHash hash = new SecureHash();
    
    tuple.append(null);
    LOG.info("Now the current url is " + tuple.get(0));
    assertEquals(null, hash.exec(tuple));
  }

  @Test
  public void testSecureHashTupleNull() throws IOException {
    SecureHash hash = new SecureHash();
    assertEquals(null, hash.exec(tuple));
  }

  @Test 
  public void testSecureHashTupleMsgIsInvalid() throws IOException {
    SecureHash hash = new SecureHash();

    tuple.append("rytbfgklm/derdtfghui/crcvtuybi");
    LOG.info("Now the current url is " + tuple.get(0));
    assertEquals("28e4978b84b25d461b144a57fddbd262", hash.exec(tuple));
  }

  @Test 
  public void testSecureHashTupleMsgIsEmpty() throws IOException {
    SecureHash hash = new SecureHash();

    tuple.append("");
    LOG.info("Now the current url is " + tuple.get(0));
    assertEquals("d41d8cd98f00b204e9800998ecf8427e", hash.exec(tuple));
  }
}
