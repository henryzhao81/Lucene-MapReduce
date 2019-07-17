package com.openx.audience.xdi.util;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by henry.zhao on 3/26/18.
 */
public class CookieStability extends Configured implements Tool {

//    private static String DataPath = "/user/dmp/xdi/stability";
//    private static String XdiPath = "/user/dmp/xdi/xdi-export";
    private static String DataPath = "/user/dmp/stability";
    private static String XdiPath = "/user/dmp/export";
    //---retro
    private static String RetroPath = "/user/dmp/cookie_distribute";
    private static String RetroOutPath = "/user/dmp/retro";
    //---
    private static long TTL = 1814400000; // milliseconds 21 * 24 * 60 * 60 * 1000

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),  new CookieStability(), args);
        System.exit(res);
    }

    public static CommandLine getCommands(String[] args) {
        CommandLineParser parser = new GnuParser();
        Options opts = createOptions();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(opts, args);
        } catch (Exception e) {
            System.err.println("failed to parse command line" + e.getMessage());
        }
        return cmd;
    }

    private static Options createOptions() {
        Options opts = new Options();
        opts.addOption("f", true, "first run date");
        opts.addOption("h", true, "aud hdfs url");
        //---retro
        opts.addOption("r", true, "is retro");
        opts.addOption("p", true, "retro result date");
        //---
        return opts;
    }

    private FileSystem getRemoteFileSystem (String hdfsUrl) throws URISyntaxException, IOException {
        URI hdfsNamenode = new URI(hdfsUrl);
        FileSystem remoteFileSystem = FileSystem.get(hdfsNamenode, new Configuration());
        return remoteFileSystem;
    }

    public boolean preRun(Path inputPath, Path outputPath) {
        try {
            Configuration conf = new Configuration();
            conf.set("mapred.input.dir.recursive", "true");
            System.out.println("input path : " + inputPath.toString() + " output path : " + outputPath.toString());
            Job job = Job.getInstance(conf, "CookieStability_Pre");
            job.setJarByClass(CookieStability.class);
            job.setReducerClass(CookieRetroResultReducer.class);
            job.setPartitionerClass(CookieStabilityPartitioner.class);
            job.setNumReduceTasks(1000);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setMapperClass(CookieRetroResultMapper.class);
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.addInputPath(job, inputPath);
            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job, outputPath);
            job.waitForCompletion(true);
        }catch(Exception ex) {
            ex.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public int run(String[] args) throws Exception {
        CommandLine cmd = getCommands(args);
        if(cmd == null)
            return -1;
        String firstDay = cmd.getOptionValue("f");
        String hdfsUrl = cmd.getOptionValue("h");//caad-o03.ca.dc.openx.org:9000 on storage grid
        //---retro
        boolean isRetro = Boolean.parseBoolean(cmd.getOptionValue("r"));
        String resultDate = cmd.getOptionValue("p");
        //---retro
        FileSystem remoteFileSystem = this.getRemoteFileSystem(hdfsUrl);

        Path xdiPath = null;
        Path prePath = null;
        Path outPath = null;
        Path retroIn1 = null;
        Path retroIn2 = null;
        Path retroOut = null;
        if(isRetro) {
            Path[] params = this.getRetroPath(firstDay, resultDate);
            retroIn1 = params[0];
            retroIn2 = params[1];
            retroOut = params[2];
        } else {
            Path[] params = this.getPaths(remoteFileSystem, firstDay);
            xdiPath = params[0];
            prePath = params[1];
            outPath = params[2];
        }
        /*
        boolean successful = preRun(retroIn2, retroOut);
        if(successful)
            return 1;
        */

        Configuration conf = new Configuration();
        conf.set("mapred.input.dir.recursive", "true");
        Job job = Job.getInstance(conf, "CookieStability");
        job.setJarByClass(CookieStability.class);
        if(isRetro && retroOut != null) {
            System.out.println("set reducer CookieRetroReducer.class");
            job.setReducerClass(CookieRetroReducer.class);
        } else {
            System.out.println("set reducer CookieStabilityReducer.class");
            job.setReducerClass(CookieStabilityReducer.class);
        }
        job.setPartitionerClass(CookieStabilityPartitioner.class);
        job.setNumReduceTasks(5000);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        if(!isRetro && xdiPath == null) {
            throw new Exception("need ad request data");
        }
        if(!isRetro && outPath == null) {
            throw new Exception("need output path");
        }
        if(prePath != null) {
            System.out.println("multiple previous path is : " + prePath.toString());
            MultipleInputs.addInputPath(job, prePath, TextInputFormat.class, CookieStabilityPreMapper.class);
            System.out.println("multiple source path is : " + xdiPath.toString());
            MultipleInputs.addInputPath(job, xdiPath, TextInputFormat.class, CookieStabilityAdsMapper.class);
        } else if(isRetro && retroIn1 != null && retroIn2 != null) {
            System.out.println("multiple pre retro path is : " + retroIn1.toString());
            MultipleInputs.addInputPath(job, retroIn1, TextInputFormat.class, CookieRetroPreMapper.class) ;
            System.out.println("multiple result retro path is : " + retroIn2.toString());
            MultipleInputs.addInputPath(job, retroIn2, TextInputFormat.class, CookieRetroResultMapper.class);
        } else {
            job.setMapperClass(CookieStabilityAdsMapper.class);
            job.setInputFormatClass(TextInputFormat.class);
            System.out.println("single source path is : " + xdiPath.toString());
            FileInputFormat.addInputPath(job, xdiPath);
        }
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        if(isRetro && retroOut != null) {
            System.out.println("pre retro out path is : " + retroOut.toString());
            FileOutputFormat.setOutputPath(job, retroOut);
        } else {
            System.out.println("out path is : " + outPath.toString());
            FileOutputFormat.setOutputPath(job, outPath);
        }
        job.waitForCompletion(true);
        if(!isRetro)
            GridSync.writeStatus(new Path(DataPath + "/" + fileStatus), outPath.getName(), remoteFileSystem);
        return 0;
    }

    private Path[] getRetroPath(String firstDay, String resultDate) throws Exception {
        String[] ymd = GridSync.split(firstDay);
        Path currentXdiPath = new Path(XdiPath + "/" + ymd[0] + "/" + ymd[1] + "/" + ymd[2]);
        Path rPath = new Path(RetroPath + "/" + resultDate);
        Path outPath = new Path(RetroOutPath + "/" + resultDate);
        return new Path[]{currentXdiPath, rPath, outPath};
    }

    private static String fileStatus = "process.status";
    /**
     * currentXdiPath (Current day of data in XDI feed need to be processed) [0]
     * previousPath (Previous day of processed data = process.status) [1]
     * targetPath (output path) [2]
     */
    private Path[] getPaths(FileSystem remoteFileSystem, String firstDay) throws Exception {
        Path[] paths = new Path[3];
        Path statusPath = new Path(DataPath + "/" + fileStatus);
        String targetXdiDay = null;
        boolean isFirstDay = false;
        if(!remoteFileSystem.exists(statusPath)) {
            if(firstDay != null && firstDay.length() > 0) {
                isFirstDay = true;
                targetXdiDay = firstDay;
                System.out.println("First Time Run");
            } else {
                throw new Exception("missing first day");
            }
        } else {
            String statusDay = GridSync.readStatus(statusPath, remoteFileSystem);
            targetXdiDay = GridSync.getNextDay(statusDay);
        }
        String[] ymd = GridSync.split(targetXdiDay);
        Path currentXdiPath = new Path(XdiPath + "/" + ymd[0] + "/" + ymd[1] + "/" + ymd[2]);
//        if(!checkXdiReady(remoteFileSystem, currentXdiPath, targetXdiDay)) {
//            throw new Exception("target xdi: "+currentXdiPath.toString() + " not ready");
//        }
        paths[0] = currentXdiPath;
        System.out.println("Current Xdi Path: " + currentXdiPath.toString());
        paths[2] = new Path(DataPath + "/" + targetXdiDay);
        System.out.println("Current Output Path: " + paths[2].toString());
        if(remoteFileSystem.exists(paths[2]))
            throw new Exception("target output path: " + paths[2].toString() + " already exit");
        System.out.println("isFirstDay : " + isFirstDay);
        if(!isFirstDay) {
            String previousDay = GridSync.getPreviousDay(targetXdiDay);
            paths[1] = new Path(DataPath + "/" + previousDay);
            if(!remoteFileSystem.exists(paths[1])) {
                throw new Exception("previous path: " + paths[1].toString() + " do not exist");
            }
        }
        return paths;
    }

    private static String lockName = "AG_XDI_EXPORT_COMPLETED";
    private boolean checkXdiReady(FileSystem remoteFileSystem, Path targetXdiPath, String targetXdiDay) throws IOException {
        if(!remoteFileSystem.exists(targetXdiPath)) {
            return false;
        }
        String lockStatus = GridSync.getStatusLock(lockName);
        System.out.println("lockStatus: " + lockStatus + " targetXdiDay : " + targetXdiDay);
        if(Integer.parseInt(targetXdiDay) < Integer.parseInt(lockStatus)) {
            return true;
        }
        return false;
    }

    public static class CookieStabilityAdsMapper extends Mapper<Object, Text, Text, Text> {

        private static int i_u_ox_id = 0;
        private static int i_ox3_trax_time = 1;
        private static int i_journal_dc = 2;
        private static int i_browser_name = 3;
        private static int i_browser_version = 4;
        private static int i_publisher_account = 5;
        private static int i_ip_address = 6;
        private static int i_geo_state = 7;
        private static int i_header = 8;

        private Text first = new Text();
        private Text second = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split("\t");
            if(words.length > 1) {

                String ox_id = ((words[i_u_ox_id].trim().length() == 0) ? "null" : words[i_u_ox_id].trim());
                String ox3_trax_time = ((words[i_ox3_trax_time].trim().length() == 0) ? "null" : words[i_ox3_trax_time].trim());
                String colo = ((words[i_journal_dc].trim().length() == 0) ? "null" : words[i_journal_dc].trim()); //datacenter
                String browser_name = ((words[i_browser_name].trim().length() == 0) ? "null" : words[i_browser_name].trim()); //browser name
                //String browser_version = ((words[i_browser_version].trim().length() == 0) ? "null" : words[i_browser_version].trim()); //browser version
                //String publisher_account = ((words[i_publisher_account].trim().length() == 0) ? "null" : words[i_publisher_account].trim()); //publisher account
                String ip_address = ((words[i_ip_address].trim().length() == 0) ? "null" : words[i_ip_address].trim()); //ip address
                //String geo_state = ((words[i_geo_state].trim().length() == 0) ? "null" : words[i_geo_state].trim()); //geo state
                //String header = ((words[i_header].trim().length() == 0) ? "null" : words[i_header].trim()); //header
                if (ox_id != null && ox_id.length() > 0 && ox3_trax_time != null && ox3_trax_time.length() > 0) {
                    first.set(ox_id);
                    second.set(ox3_trax_time + "\t" + colo + "\t" + browser_name + "\t" + ip_address);
                    context.write(first, second);
                }
                /*
                String ox_id = ((words[i_u_ox_id].trim().length() == 0) ? "null" : words[i_u_ox_id].trim());
                String browser_name = ((words[i_browser_name].trim().length() == 0) ? "null" : words[i_browser_name].trim()); //browser name
                String browser_version = ((words[i_browser_version].trim().length() == 0) ? "null" : words[i_browser_version].trim()); //browser version
                if (ox_id != null && ox_id.length() > 0 && browser_name != null && browser_name.length() > 0 && browser_version != null && browser_version.length() > 0) {
                    first.set(ox_id + "_" + browser_name + "_" + browser_version);
                }
                String ox3_trax_time = ((words[i_ox3_trax_time].trim().length() == 0) ? "null" : words[i_ox3_trax_time].trim());
                String colo = ((words[i_journal_dc].trim().length() == 0) ? "null" : words[i_journal_dc].trim()); //datacenter
                second.set(ox3_trax_time + "\t" + colo);
                context.write(first, second);
                */
            }
        }
    }

    public static long toUnixTime(String time) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss");
            Date date = sdf.parse(time.trim());
            long timestamp = date.getTime();
            return timestamp;
        }catch(Exception ex) {
            System.out.println("failed to convert " + time + " to timestamp" + ex.getMessage());
            ex.printStackTrace();
        }
        return -1;
    }

    public static class CookieStabilityPreMapper extends Mapper<Object, Text, Text, Text> {

        private static int m_u_ox_id = 0;
        private static int m_ox3_trax_time = 1;
        private static int m_journal_dc = 2;
        //private static int m_browser_name = 3;
        //private static int m_ip_address = 4;
        //private static int m_geo_state = 5;

        Text first = new Text();
        Text second = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split("\t");
            if(words.length > 1) {
                String strKey = ((words[m_u_ox_id].trim().length() == 0) ? "null" : words[m_u_ox_id].trim()); //uid
                String strValue = ((words[m_ox3_trax_time].trim().length() == 0) ? "null" : words[m_ox3_trax_time].trim()); //startTime,endTime,times
                String colo = ((words[m_journal_dc].trim().length() == 0) ? "null" : words[m_journal_dc].trim()); //datacenter
                //String brname = ((words[m_browser_name].trim().length() == 0) ? "null" : words[m_browser_name].trim()); //browser name
                //String ipaddress = ((words[m_ip_address].trim().length() == 0) ? "null" : words[m_ip_address].trim()); //ip address
                //String state = ((words[m_geo_state].trim().length() == 0) ? "null" : words[m_geo_state].trim()); //state
                if(strKey != null && strKey.length() > 0 && strValue != null && strValue.length() > 0) {
                    first.set(strKey);
                    second.set(strValue + "\t" + colo);
                    context.write(first, second);
                }
            }
        }
    }

    public static class CookieRetroPreMapper extends  Mapper<Object, Text, Text, Text> {
        private static int m_u_ox_id = 0;
        private static int m_ox3_trax_time = 1;
        private static int m_journal_dc = 2;
        private static int m_browser_name = 3;
        private static int m_browser_version = 4;
        private static int m_publisher_id = 5;
        private static int m_ip_address = 6;
        private static int m_geo_state = 7;
        private static int m_header = 8;

        Text first = new Text();
        Text second = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split("\t");
            if(words.length > 1) {
                String strKey = ((words[m_u_ox_id].trim().length() == 0) ? "null" : words[m_u_ox_id].trim()); //uid
                String timestamp = ((words[m_ox3_trax_time].trim().length() == 0) ? "null" : words[m_ox3_trax_time].trim()); //timestamp
                String colo = ((words[m_journal_dc].trim().length() == 0) ? "null" : words[m_journal_dc].trim()); //datacenter
                String brName = ((words[m_browser_name].trim().length() == 0) ? "null" : words[m_browser_name].trim()); //browser name
                String brVersion = ((words[m_browser_version].trim().length() == 0) ? "null" : words[m_browser_version].trim()); //browser version
                String pubId = ((words[m_publisher_id].trim().length() == 0) ? "null" : words[m_publisher_id].trim()); //publisher id
                String ipAddress = ((words[m_ip_address].trim().length() == 0) ? "null" : words[m_ip_address].trim()); //ip address
                String state = ((words[m_geo_state].trim().length() == 0) ? "null" : words[m_geo_state].trim()); //state
                String header = ((words[m_header].trim().length() == 0) ? "null" : words[m_header].trim()); //header
                if(strKey != null && strKey.length() > 0) {
                    first.set(strKey);
                    second.set(timestamp + "\t" + colo + "\t" + brName + "\t" + brVersion + "\t" + pubId + "\t" + ipAddress + "\t" + state + "\t" + header);
                    context.write(first, second);
                }
            }
        }
    }

    public static class CookieRetroResultMapper extends  Mapper<Object, Text, Text, Text> {
        private static int m_u_ox_id = 0;
        private static int m_total_count = 1;
        private static int m_daily_count = 2;

        Text first = new Text();
        Text second = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split("\t");
            if(words.length > 1) {
                String strKey = ((words[m_u_ox_id].trim().length() == 0) ? "null" : words[m_u_ox_id].trim()); //uid
                String totalCount = ((words[m_total_count].trim().length() == 0) ? "null" : words[m_total_count].trim());
                String dailyCount = ((words[m_daily_count].trim().length() == 0) ? "null" : words[m_daily_count].trim());
                if(strKey != null && strKey.length() > 0 && dailyCount != null && dailyCount.equals("0_0_0_0_0_0")) {
                    first.set(strKey);
                    second.set(totalCount + "\t" + "filtered");
                    context.write(first, second);
                }
            }
        }
    }


    public static class CookieRetroResultReducer extends Reducer<Text, Text, Text, Text> {

        long totalUniqueCount = 0;
        long totalCount = 0;
        long totalFilteredCount = 0;
        long totalUnFilteredCount = 0;
        int maxCount = 0;
        private int taskId = -1;
        private String strKey = null;

        @Override
        protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException, InterruptedException {
            this.taskId = context.getTaskAttemptID().getTaskID().getId();

        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for(Text value : values) {
                String[] terms = value.toString().split("\t");
                if(terms!= null && terms.length == 2 && terms[1].equals("filtered")) {
                    count += Integer.parseInt(terms[0]);
                    totalFilteredCount++;
                } else {
                    totalUnFilteredCount++;
                }
                totalCount++;
            }
            if(count > this.maxCount) {
                this.strKey = key.toString();
                this.maxCount = count;
            }
            totalUniqueCount++;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            System.out.println("totalUniqueCount : " + totalUniqueCount);
            System.out.println("totalCount : " + totalCount);
            System.out.println("totalFilteredCount : " + totalFilteredCount);
            System.out.println("totalUnFilteredCount : " + totalUnFilteredCount);
            System.out.println("set context key : retro_" + this.taskId + " value : " + this.maxCount + " with " + this.strKey);
            context.getConfiguration().set("retro_"+this.taskId, Integer.toString(this.maxCount));
        }
    }

    public static class CookieRetroReducer extends Reducer<Text, Text, Text, Text> {
        /*
        private static int r_trax_time = 0;
        private static int r_journal_dc = 1;
        private static int r_browser_name = 2;
        private static int r_browser_version = 3;
        private static int r_publisher_id = 4;
        private static int r_ip_address = 5;
        private static int r_geo_state = 6;
        private static int r_header = 7;
        */
        private Text result = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> resultList = new ArrayList<>();
            boolean matched = false;
            int count = 0;
            for(Text value : values) {
                if(count > 10)
                    break;
                String[] terms = value.toString().split("\t");
                if(terms!= null && terms.length == 2 && terms[1].equals("filtered")) {
                    matched = true;
                } else if(terms != null && terms.length > 7){
                    resultList.add(value.toString());
                }
                count++;
            }
            if(matched) {
                if(resultList.size() > 0) {
                    for(String each : resultList) {
                        result.set(each);
                        context.write(key, result);
                    }
                    resultList.clear();
                }
            }
        }
    }


    public static class CookieStabilityReducer extends Reducer<Text, Text, Text, Text> {
        private static int r_ox3_trax_time = 0;
        private static int r_journal_dc = 1;
        private static int r_browser_name = 2;
        private static int r_ip_address = 3;
        //private static int r_geo_state = 4;
        private Text result = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            long start = -1;
            long end = -1;
            Map<String, Integer> dcSet = new HashMap<>();
            Map<String, Integer> brSet = new HashMap<>();
            Map<String, Integer> ipSet = new HashMap<>();
            //Map<String, Integer> gsSet = new HashMap<>();
            for(Text value : values) {
                String[] terms = value.toString().split("\t");
                String timestamp = terms[r_ox3_trax_time];
                String datacenter = terms[r_journal_dc];
                /*
                String nbrowser = terms[r_browser_name];
                String ipaddress = terms[r_ip_address];
                */
                //String geostate = terms[r_geo_state];
                if(timestamp.contains(",")) { // data from CookieStabilityPreMapper
                    String[] vals = timestamp.split(","); //vals[0]: startTime, vals[1]: endTime, vals[2]: times
                    long currStart = Long.parseLong(vals[0]);
                    long currEnd = Long.parseLong(vals[1]);
                    if(start < 0) {
                        start = currStart;
                        end = currEnd;
                    } else {
                        if(currStart < start) {
                            start = currStart;
                        }
                        if(currEnd > end) {
                            end = currEnd;
                        }
                    }
                    count = count + Integer.parseInt(vals[2]);
                } else { // data from CookieStabilityAdsMapper
                    count++;
                    long curr = Long.parseLong(timestamp);
                    if(start < 0) {
                        start = curr;
                        end = curr;
                    } else {
                        if(curr > end) {
                            end = curr;
                        }
                        if(curr < start) {
                            start = curr;
                        }
                    }
                }
                if(datacenter.contains(",")) { // data from CookieStabilityPreMapper
                    String[] vals = datacenter.split(","); //XV_10,LC_5,CA_2
                    for(String val : vals) {
                        String[] pair = val.split("_");
                        if(pair.length != 2)
                            throw new IOException("Wrong format: " + val);
                        String keyDc = pair[0];
                        Integer valueCount = Integer.parseInt(pair[1]);
                        if(!dcSet.containsKey(keyDc)) {
                            dcSet.put(keyDc, valueCount);
                        } else {
                            Integer currentValue = dcSet.get(keyDc);
                            dcSet.put(keyDc, (currentValue + valueCount));
                        }
                    }
                } else { // could be XV or LC only or XV_1
                    if(datacenter.contains("_")) {
                        String[] pair = datacenter.split("_");
                        if(pair.length != 2)
                            throw new IOException("Wrong format: " + datacenter);
                        String strDc = pair[0];
                        Integer valueCount = Integer.parseInt(pair[1]);
                        if(!dcSet.containsKey(strDc)) {
                            dcSet.put(strDc, valueCount);
                        } else {
                            Integer currentValue = dcSet.get(strDc);
                            dcSet.put(strDc, (currentValue + valueCount));
                        }
                    } else {
                        if(!dcSet.containsKey(datacenter)) {
                            dcSet.put(datacenter, 1);
                        } else {
                            Integer currentValue = dcSet.get(datacenter);
                            dcSet.put(datacenter, (currentValue + 1));
                        }
                    }
                }

                //browser
                /*
                if(nbrowser.contains(",")) { // data from CookieStabilityPreMapper
                    String[] vals = nbrowser.split(","); //chrom_10,firefox_5
                    for(String val : vals) {
                        String[] pair = val.split("_");
                        if(pair.length != 2)
                            throw new IOException("Wrong format: " + val);
                        String keyDc = pair[0];
                        Integer valueCount = Integer.parseInt(pair[1]);
                        if(!brSet.containsKey(keyDc)) {
                            brSet.put(keyDc, valueCount);
                        } else {
                            Integer currentValue = brSet.get(keyDc);
                            brSet.put(keyDc, (currentValue + valueCount));
                        }
                    }
                } else { // could be chrome or firefox or only chrome_1
                    if(nbrowser.contains("_")) {
                        String[] pair = nbrowser.split("_");
                        if(pair.length != 2)
                            throw new IOException("Wrong format: " + nbrowser);
                        String strDc = pair[0];
                        Integer valueCount = Integer.parseInt(pair[1]);
                        if(!brSet.containsKey(strDc)) {
                            brSet.put(strDc, valueCount);
                        } else {
                            Integer currentValue = brSet.get(strDc);
                            brSet.put(strDc, (currentValue + valueCount));
                        }
                    } else {
                        if(!brSet.containsKey(nbrowser)) {
                            brSet.put(nbrowser, 1);
                        } else {
                            Integer currentValue = brSet.get(nbrowser);
                            brSet.put(nbrowser, (currentValue + 1));
                        }
                    }
                }
                //ip address
                if(ipaddress.contains(",")) { // data from CookieStabilityPreMapper
                    String[] vals = ipaddress.split(",");
                    for(String val : vals) {
                        String[] pair = val.split("_");
                        if(pair.length != 2)
                            throw new IOException("Wrong format: " + val);
                        String keyDc = pair[0];
                        Integer valueCount = Integer.parseInt(pair[1]);
                        if(!ipSet.containsKey(keyDc)) {
                            ipSet.put(keyDc, valueCount);
                        } else {
                            Integer currentValue = ipSet.get(keyDc);
                            ipSet.put(keyDc, (currentValue + valueCount));
                        }
                    }
                } else {
                    if(ipaddress.contains("_")) {
                        String[] pair = ipaddress.split("_");
                        if(pair.length != 2)
                            throw new IOException("Wrong format: " + ipaddress);
                        String strDc = pair[0];
                        Integer valueCount = Integer.parseInt(pair[1]);
                        if(!ipSet.containsKey(strDc)) {
                            ipSet.put(strDc, valueCount);
                        } else {
                            Integer currentValue = ipSet.get(strDc);
                            ipSet.put(strDc, (currentValue + valueCount));
                        }
                    } else {
                        if(!ipSet.containsKey(ipaddress)) {
                            ipSet.put(ipaddress, 1);
                        } else {
                            Integer currentValue = ipSet.get(ipaddress);
                            ipSet.put(ipaddress, (currentValue + 1));
                        }
                    }
                }
                */

                //state
                /*
                if(geostate.contains(";")) { // data from CookieStabilityPreMapper
                    String[] vals = geostate.split(";");
                    for(String val : vals) {
                        String[] pair = val.split("_");
                        if(pair.length != 2)
                            throw new IOException("Wrong format: " + val);
                        String keyDc = pair[0];
                        Integer valueCount = Integer.parseInt(pair[1]);
                        if(!gsSet.containsKey(keyDc)) {
                            gsSet.put(keyDc, valueCount);
                        } else {
                            Integer currentValue = gsSet.get(keyDc);
                            gsSet.put(keyDc, (currentValue + valueCount));
                        }
                    }
                } else {
                    if(geostate.contains("_")) {
                        String[] pair = geostate.split("_");
                        if(pair.length != 2)
                            throw new IOException("Wrong format: " + geostate);
                        String strDc = pair[0];
                        Integer valueCount = Integer.parseInt(pair[1]);
                        if(!gsSet.containsKey(strDc)) {
                            gsSet.put(strDc, valueCount);
                        } else {
                            Integer currentValue = gsSet.get(strDc);
                            gsSet.put(strDc, (currentValue + valueCount));
                        }
                    } else {
                        if(!gsSet.containsKey(geostate)) {
                            gsSet.put(geostate, 1);
                        } else {
                            Integer currentValue = gsSet.get(geostate);
                            gsSet.put(geostate, (currentValue + 1));
                        }
                    }
                }
                */
            }
            if(start > end)
                throw new IOException("start ("+start+") > end ("+end+")");
            StringBuffer buffer = new StringBuffer();
            buffer.append(start).append(",").append(end).append(",").append(count);
            //data center
            StringBuffer dcBuffer = new StringBuffer();
            int dcSize = dcSet.size();
            int c = 0;
            for(Map.Entry<String, Integer> each : dcSet.entrySet()) {
                if(c >= (dcSize - 1))
                    dcBuffer.append(each.getKey() + "_" + each.getValue());
                else
                    dcBuffer.append(each.getKey() + "_" + each.getValue()).append(",");
                c++;
            }

            //browser
            /*
            StringBuffer brBuffer = new StringBuffer();
            int brSize = brSet.size();
            c = 0;
            for(Map.Entry<String, Integer> each : brSet.entrySet()) {
                if(c >= (brSize - 1))
                    brBuffer.append(each.getKey() + "_" + each.getValue());
                else
                    brBuffer.append(each.getKey() + "_" + each.getValue()).append(",");
                c++;
            }
            //ip address
            StringBuffer ipBuffer = new StringBuffer();
            int ipSize = ipSet.size();
            c = 0;
            for(Map.Entry<String, Integer> each : ipSet.entrySet()) {
                if(c >= (ipSize - 1))
                    ipBuffer.append(each.getKey() + "_" + each.getValue());
                else
                    ipBuffer.append(each.getKey() + "_" + each.getValue()).append(",");
                c++;
            }
            */
            //state
//            StringBuffer gsBuffer = new StringBuffer();
//            int gsSize = gsSet.size();
//            c = 0;
//            for(Map.Entry<String, Integer> each : gsSet.entrySet()) {
//                if(c >= (gsSize - 1))
//                    gsBuffer.append(each.getKey() + "_" + each.getValue());
//                else
//                    gsBuffer.append(each.getKey() + "_" + each.getValue()).append(";");
//                c++;
//            }
            //result.set(buffer.toString() + "\t" + dcBuffer.toString() + "\t" + brBuffer.toString() + "\t" + ipBuffer.toString() + "\t" + gsBuffer.toString());
            result.set(buffer.toString() + "\t" + dcBuffer.toString());
            context.write(key, result);
        }
    }

    private static boolean isExpired(long time) {
        if((System.currentTimeMillis() - time) >= TTL)
            return true;
        return false;
    }

    public static class CookieStabilityPartitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            Integer in = key.toString().hashCode();
            int id = (int)(Math.abs(in.longValue()) % numPartitions);
            return id;
        }
    }

}
