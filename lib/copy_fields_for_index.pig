-- input param 1: INPUT_DIR
-- input param 2: OUTPUT_DIR
-- input param 3: JAR_FILE
-- input param 4: OUTPUT_FILE_SIZE

-- the default output file size is 1 GB before compression
-- after gzip, it could become about 300~400 MB
%default OUTPUT_FILE_SIZE 1073741824;

REGISTER $JAR_FILE
DEFINE SequenceFileLoader com.openx.audience.xdi.pig.SequenceFileLoader();

-- generate compressed gzip files for output
SET pig.maxCombinedSplitSize $OUTPUT_FILE_SIZE;
SET output.compression.enabled true;
SET output.compression.codec org.apache.hadoop.io.compress.GzipCodec;

--remove the final output folder if it exists
rmf $OUTPUT_DIR

a = LOAD '$INPUT_DIR' USING SequenceFileLoader() AS
(
  key:bytearray, -- emtpy key from Hive SequenceFile format
  value:chararray
);

mr = FOREACH a GENERATE FLATTEN( STRSPLIT(value,'\t') ) AS
(
  e_ox3_trax_time:chararray,
  e_ox3_trax_id:chararray,
  u_ip_addr:chararray,
  u_viewer_id:chararray,
  u_ox_id:chararray,
  u_external_id:chararray,
  u_mobl_dev_id:chararray,
  u_mobl_dev_id_type:chararray,
  u_header_ua:chararray,
  u_can_cookie:chararray,
  u_new_viewer:chararray,
  u_mkt_can_cookie:chararray,
  u_mkt_new_viewer:chararray,
  u_mkt_cookie_age:chararray,
  u_browser_name:chararray,
  u_os:chararray,
  u_device_name:chararray,
  u_geo_dma:chararray,
  u_geo_state:chararray,
  u_geo_city:chararray,
  u_geo_lat:chararray,
  u_geo_lon:chararray,
  u_geo_zip:chararray,
  p_req_deliv_medium:chararray,
  p_account:chararray,
  u_page_url:chararray,
  p_site:chararray,
  p_site_category_1:chararray,
  p_site_category_2:chararray,
  u_refer_url:chararray
);

o = FOREACH mr GENERATE
  u_viewer_id,
  u_ox_id,
  u_mkt_can_cookie,
  u_browser_name;

uniq = DISTINCT o;

--save to file
STORE uniq INTO '$OUTPUT_DIR' USING PigStorage('\u0001');
