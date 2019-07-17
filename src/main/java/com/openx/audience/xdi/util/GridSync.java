package com.openx.audience.xdi.util;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptionSwitch;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.tools.OptionsParser;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

/**
 * Created by henry.zhao on 1/25/17.
 */
public class GridSync {

    String statusFile; // -s="/user/dmp/media-math/sync/sync.status"
    String gridHdfsURL; // -g="hdfs://xvaja-db-01.xv.dc.openx.org:8020"
    String audHdfsURL; // -a="hdfs://xvaic-sg-36.xv.dc.openx.org:8020"
    String gridPath;  // -o="/grid/ox3-seg/archives-hourly/OXS"
    String audPath;   // -t="/user/dmp/media-math/sync"
    String gridLock; // -l="OXS_HOURLY_ARCHIVE_BUILDER"

    public GridSync(String statusFile, String gridHdfsURL, String audHdfsURL,  String gridPath, String audPath, String gridLock) {
        this.statusFile = statusFile;
        this.gridHdfsURL = gridHdfsURL;
        this.audHdfsURL = audHdfsURL;
        this.gridPath = gridPath;
        this.audPath = audPath;
        this.gridLock = gridLock;
    }

    private static Options createOptions() {
        Options opts = new Options();
        opts.addOption("s", true, "status file");
        opts.addOption("g", true, "grid hdfs url");
        opts.addOption("a", true, "aud hdfs url");
        opts.addOption("o", true, "origin grid path");
        opts.addOption("t", true, "target aud path");
        opts.addOption("l", true, "grid url lock name");
        return opts;
    }

    public static CommandLine getCommands(String[] args) {
        CommandLineParser parser = new GnuParser();
        HelpFormatter formatter = new HelpFormatter();
        Options opts = createOptions();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(opts, args);
            if ( !cmd.hasOption("s") || !cmd.hasOption("g") || !cmd.hasOption("a")
                    || !cmd.hasOption("o") || !cmd.hasOption("t") || !cmd.hasOption("l")) {
                System.err.println("parameter -s -g -a -o -t -l is required");
                return null;
            }
        } catch (Exception e) {
            System.err.println("failed to parse command line" + e.getMessage());
        }
        return cmd;
    }

    public void remoteSync() {
        try {
            System.out.println("aud hdfs url : " + audHdfsURL);
            FileSystem audFileSystem = this.getRemoteFileSystem(audHdfsURL);
            System.out.println("grid hdfs url : " + gridHdfsURL);
            FileSystem gridFileSystem = this.getRemoteFileSystem(gridHdfsURL);
            Path statusFilePath = new Path(statusFile);
            String lastUpdateStatus = null;
            if(audFileSystem.exists(statusFilePath)) {
                lastUpdateStatus = readStatus(statusFilePath, audFileSystem);
                System.out.println("aud grid last update status is : " + lastUpdateStatus);
            } else {
                System.out.println("first time sync, not status file created");
            }
            String[] results = this.getGridSourceReady(gridFileSystem, gridPath, gridLock);
            String lastSource = null;
            String resultLastPath = null;
            if(results != null && results.length > 1) {
                resultLastPath = results[0];
                lastSource = results[1];
            } else {
                System.err.println("failed to get grid source path and status !");
                return;
            }
            if (lastSource != null && lastSource.length() > 0 && resultLastPath != null && resultLastPath.length() > 0) {
                if (lastUpdateStatus == null
                        || Integer.parseInt(lastUpdateStatus) < Integer.parseInt(lastSource)) {
                    System.out.println("start to sync from " + resultLastPath + " to " + audPath);
                    String completePath = audPath + "/" + lastSource;
                    System.out.println("check if completePath exit");
                    Path c_path = new Path(completePath);
                    if (audFileSystem.exists(c_path)) {
                        System.out.println("delete existing path " + completePath);
                        audFileSystem.delete(c_path, true);
                    }
                    if (audFileSystem.exists(c_path)) {
                        throw new Exception("Failed to delete path " + completePath);
                    }
                    System.out.println("copy from " + gridHdfsURL + resultLastPath + "/*" + " to " + audHdfsURL + completePath + "/");
                    DistCpOptions options = OptionsParser.parse(new String[]{gridHdfsURL + resultLastPath + "/*", audHdfsURL + completePath + "/"});
                    new DistCp(new Configuration(), options).execute();
                    writeStatus(statusFilePath, lastSource, audFileSystem);
                } else {
                    System.out.println("lastUpdateStatus : " + lastUpdateStatus + " is no later than last grid source " + lastSource + " no need sync");
                }
            } else {
                System.err.println("ERROR : cannot get last grid source, stop sync!");
            }
            //}
        }catch(Exception ex) {
            System.out.println("failed to sync with remote server " + ex.getMessage());
            ex.printStackTrace();
        }
    }


    private FileSystem getRemoteFileSystem (String hdfsUrl) throws URISyntaxException, IOException {
        URI hdfsNamenode = new URI(hdfsUrl);
        FileSystem remoteFileSystem = FileSystem.get(hdfsNamenode, new Configuration());
        return remoteFileSystem;
    }

    /**
     *
     * @param remoteFileSystem
     * @param strSourcePath  /grid/ox3-seg/archives-hourly/OXS/
     * @param lockName OXS_HOURLY_ARCHIVE_BUILDER
     * @return
     */
    private String[] getGridSourceReady(FileSystem remoteFileSystem, String strSourcePath, String lockName) {
        try {
            Path latestPath = this.findLatest(remoteFileSystem, strSourcePath);
            System.out.println("the latest path in grid hdfs is : " + latestPath.toString());
            String[] splits  = latestPath.toString().split("/");
            String year = null;
            String month = null;
            String day = null;
            for(String split : splits) {
                if(split.contains("y=")){
                    int y = Integer.parseInt(split.substring(split.indexOf("y=") + 2));
                    year = Integer.toString(y);
                } else if(split.contains("m=")) {
                    int m = Integer.parseInt(split.substring(split.indexOf("m=") + 2));
                    if(m < 10)
                        month = "0" + m;
                    else
                        month = Integer.toString(m);
                } else if(split.contains("d=")) {
                    int d = Integer.parseInt(split.substring(split.indexOf("d=") + 2));
                    if(d < 10)
                        day = "0" + d;
                    else
                        day = Integer.toString(d);
                }
            }
            String tSource = null;
            if(year != null && month != null && day != null)
                tSource = year + month + day;
            String tStatus = getStatusLock(lockName);
            System.out.println("current last path in grid is : " + tSource + " lock is : " + tStatus);
            if(tSource != null && tStatus != null) {
                String[] results = new String[2];
                if(Integer.parseInt(tSource) < Integer.parseInt(tStatus)) {
                    results[0] = latestPath.toString();
                    results[1] = tSource;
                    System.out.println("grid source is ready, last source date is "
                            + results[0] + " last source path is : " + results[1]);
                } else {
                    System.out.println("latest source have not complete generated on grid, get previous day");
                    String previous_path = getPreviousDayPath(strSourcePath, tSource);
                    tSource = getPreviousDay(tSource);
                    System.out.println("current last path in grid is : " + tSource + " lock is : " + tStatus);
                    if(Integer.parseInt(tSource) < Integer.parseInt(tStatus)) {
                        results[0] = previous_path;
                        results[1] = tSource;
                        System.out.println("grid source is ready, last source date is "
                                + results[0] + " last source path is : " + results[1]);
                    } else {
                        System.err.println("gap between tSource" + tSource + " and tStatus " + tStatus);
                    }
                }
                return results;
            } else {
                System.err.println("tSource or tStatus is null");
            }
        }catch(Exception ex) {
            System.err.println("failed to check grid source is ready " + ex.getMessage());
        }
        return null;
    }

    public static String getPreviousDay(String strDay) throws Exception {
        if(strDay.length() != 8)
            throw new Exception("illedge format : " + strDay + " 8 digits");
        String tempTime = strDay;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Calendar c = Calendar.getInstance();
        c.setTime(sdf.parse(tempTime));
        c.add(Calendar.DATE, -1);
        tempTime = sdf.format(c.getTime());
        return tempTime;
    }

    public static String getNextDay(String strDay) throws Exception {
        if(strDay.length() != 8)
            throw new Exception("illedge format : " + strDay + " 8 digits");
        String tempTime = strDay;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Calendar c = Calendar.getInstance();
        c.setTime(sdf.parse(tempTime));
        c.add(Calendar.DATE, 1);
        tempTime = sdf.format(c.getTime());
        return tempTime;
    }

    public static String getPreviousDayPath(String gridPath, String strDay) throws Exception {
        if(strDay.length() != 8)
            throw new Exception("illedge format : " + strDay + " 8 digits");
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Calendar c = Calendar.getInstance();
        c.setTime(sdf.parse(strDay));
        c.add(Calendar.DATE, -1);
        int year = c.get(Calendar.YEAR);
        int month = c.get(Calendar.MONTH) + 1; //JAN is 0
        int day = c.get(Calendar.DAY_OF_MONTH); //First is 1
        String strPath = gridPath + "/y=" + year + "/m=" + (month < 10 ? "0"+month : Integer.toString(month)) + "/d=" + (day < 10 ? "0"+day : Integer.toString(day));
        return strPath;
    }
    
    /**
     * @param strYmd  : 20171101
     * @return
     * @throws Exception
     */
    public static String[] split(String strYmd) throws Exception {
        if(strYmd.length() != 8)
            throw new Exception("illedge format : " + strYmd + " 8 digits");
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Calendar c = Calendar.getInstance();
        c.setTime(sdf.parse(strYmd));
        int year = c.get(Calendar.YEAR);
        int month = c.get(Calendar.MONTH) + 1; //JAN is 0
        int day = c.get(Calendar.DAY_OF_MONTH); //First day is 1
        String[] result = new String[3];
        result[0] = "y=" + year;
        result[1] = "m=" + (month < 10 ? "0"+month : Integer.toString(month));
        result[2] = "d=" + (day < 10 ? "0"+day : Integer.toString(day));
        return result;
    }

    public static void main(String[] args) {
        CommandLine cmd = getCommands(args);
        if(cmd == null) {
            return;
        }
        String sFile = cmd.getOptionValue("s");
        String gHdfs = cmd.getOptionValue("g");
        String aHdfs = cmd.getOptionValue("a");
        String gPath = cmd.getOptionValue("o");
        String aPath = cmd.getOptionValue("t");
        String sLock = cmd.getOptionValue("l");
        System.out.println("status file : " + sFile + " grid hdfs : " + gHdfs + " aud hdfs : " + aHdfs + " grid path : " + gPath + " aud path : " + aPath + " lock name : " + sLock);      
        GridSync sync = new GridSync(sFile, gHdfs, aHdfs, gPath, aPath, sLock);
        sync.remoteSync();
    }

    /**
     * status format is like 20170101 <year><month><day>
     * @param source
     * @param remoteFileSystem
     * @return
     */
    public static String readStatus(Path source, FileSystem remoteFileSystem){
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(remoteFileSystem.open(source)));
            String line = br.readLine();
            return line;
        }catch(IOException ex) {
            ex.printStackTrace();
        }finally {
            if(br != null) {
                try {
                    br.close();
                }catch(IOException ex) {
                    ex.printStackTrace();
                }
            }
        }
        return null;
    }

    /**
     *
     * @param statusFile
     * @param updatedStatus status format is like 20170101 <year><month><day>
     * @param remoteFileSystem
     * @throws Exception
     */
    public static void writeStatus(Path statusFile, String updatedStatus, FileSystem remoteFileSystem) throws Exception {
        if(remoteFileSystem.exists(statusFile)) {
            remoteFileSystem.delete(statusFile, false);
            if(remoteFileSystem.exists(statusFile)) {
                throw new Exception("Failed to overwrite status file");
            }
        }
        BufferedWriter br = null;
        try {
            br = new BufferedWriter(new OutputStreamWriter(remoteFileSystem.create(statusFile,true)));
            br.write(updatedStatus);
        } finally {
            br.close();
        }
    }

    /**
     * Find latest path in grid hdfs
     * @param remoteFileSystem gird hdfs file system
     * @param strPath
     * @return
     */
    private Path findLatest(FileSystem remoteFileSystem, String strPath) {
          try {
            Path tPath = new Path(strPath, "*");
            final FileStatus[] status = remoteFileSystem.globStatus(tPath);
            if(status == null || status.length == 0) {
                System.out.println("empty source updated path : " + strPath);
            } else {
                Path latestPath = this.findLatestPathInGrid(remoteFileSystem, tPath);
                System.out.println("lastest path in Grid is " + latestPath);
                return latestPath;
            }
        }catch(Exception ex) {
            System.out.println("failed to find complete date " + ex.getMessage());
            ex.printStackTrace();
        }
        return null;
    }

    /**
     * Find latest path in grid
     * @param remoteFileSystem
     * @param tPath
     * @return
     * @throws Exception
     */
    private Path findLatestPathInGrid(FileSystem remoteFileSystem, Path tPath) throws Exception {
        final FileStatus[] status = remoteFileSystem.globStatus(tPath);
        if(status == null || status.length == 0)
            return null;
        List<Integer> num = new ArrayList<Integer>();
        String b_str = "";
        for(FileStatus stat : status) {
            Path y = stat.getPath();
            String p = y.getName();
            String c_str = p.substring(p.indexOf("=") + 1);
            b_str = p.substring(0, p.indexOf("="));
            num.add(Integer.parseInt(c_str));
        }
        Collections.sort(num, Collections.reverseOrder());
        int next = num.get(0);
        String str_next = "" + next;
        if(next < 10) {
            str_next = "0" + next;
        }
        String strPath = tPath.toString();
        if(strPath.endsWith("*"))
            strPath = strPath.replace("*", b_str + "=" + str_next);
        else
            strPath = tPath.toString() + "/" + b_str + "=" + str_next;
        String[] s_str = strPath.split("/");
        String lastSplit = s_str[s_str.length - 1];
        if(lastSplit.contains("d="))
            return new Path(strPath);
        else
            return findLatestPathInGrid(remoteFileSystem, new Path(strPath, "*"));
    }

    private static final String LOCK_URL = "http://prod-cdh-grid-locks-rest.xv.dc.openx.org:8080/locks/query/";
    public static String getStatusLock(String lockName) {
        BufferedReader rd = null;
        String time = null;
        try {
            URL url = new URL(LOCK_URL + lockName);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(60000);
            rd = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line = "", data = "";
            while ((line = rd.readLine()) != null) {
                data += line;
            }
            System.out.println(data);
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(new InputSource(new StringReader(data)));
            doc.getDocumentElement().normalize();
            NodeList nList = doc.getElementsByTagName("Lock");
            for (int temp = 0; temp < nList.getLength(); temp++) {
                Node nNode = nList.item(temp);
                System.out.println("\nCurrent Element :" + nNode.getNodeName());
                if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element eElement = (Element) nNode;
                    String name = eElement.getElementsByTagName("name").item(0).getTextContent();
                    if(name.equals(lockName)) {
                        time = eElement.getElementsByTagName("time").item(0).getTextContent();
                        break;
                    }
                }
            }
            rd.close();
        }catch(Exception ex) {
            ex.printStackTrace();
        } finally {
            try {
                if(rd != null)
                    rd.close();
            }catch(Exception e) {
                e.printStackTrace();
            }
        }
        String result = null;
        if(time != null && time.length() > 0) {
            try {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'");
                Date date = sdf.parse(time);
                Calendar calendar = Calendar.getInstance();
                calendar.setTime(date);
                int year = calendar.get(Calendar.YEAR);
                int month = calendar.get(Calendar.MONDAY) + 1; //JAN is 0
                int day = calendar.get(Calendar.DAY_OF_MONTH); //First is 1
                result = year + (month < 10 ? "0"+month : Integer.toString(month)) + (day < 10 ? "0"+day : Integer.toString(day));
            }catch(Exception e) {
                e.printStackTrace();
            }
        }
        return result;
    }
}
