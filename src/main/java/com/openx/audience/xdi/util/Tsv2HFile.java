package com.openx.audience.xdi.util;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.ImportTsv;
import org.apache.hadoop.util.ToolRunner;

/**
 * For general purpose of using hfile gen
 */
public class Tsv2HFile {

  public static CommandLine getCommandLines(String[] args) {
    CommandLineParser parser = new GnuParser();
    Options opts = createOptions();
    CommandLine cmd = null;
    try {
        cmd = parser.parse(opts, args);
        if ( !cmd.hasOption("i") || !cmd.hasOption("o") || !cmd.hasOption("t") 
            || !cmd.hasOption("c") || !cmd.hasOption("zq")) {
            System.err.println("parameter -i -o -zq -t -c is required");
            return null;
        }
    } catch (Exception e) {
        System.err.println("failed to parse command line");
    }
    return cmd;
}

/**
 * @return
 */
  private static Options createOptions() {
    Options opts = new Options();
    opts.addOption("t", true, "table name");
    opts.addOption("c", true, "import columns");
    opts.addOption("i", true, "input folder");
    opts.addOption("o", true, "output folder");
    opts.addOption("zq", true, "hbase zookeeper quorums");
    return opts;
  }
  
  public static void main(String[] args) throws IOException, URISyntaxException {
    CommandLine cmd = getCommandLines(args);
    if (cmd == null) {
      return;
    }

    String inputFolder = cmd.getOptionValue("i");
    String outputFolder = cmd.getOptionValue("o");
    String zkQuorum = cmd.getOptionValue("zq");
    String tableName = cmd.getOptionValue("t");
    String columns = cmd.getOptionValue("c");
    
    Tsv2HFile t2h = new Tsv2HFile(inputFolder, outputFolder, zkQuorum, tableName, columns);
    t2h.load();
  }

  private String inputFolder;
  private String outputFolder;
  private String zkQuorum;
  private String tableName;
  private String columns;

  public Tsv2HFile(String inputFolder, String outputFolder, String zkQuorum, String tableName, String columns) {
    this.inputFolder = inputFolder;
    this.outputFolder = outputFolder;
    this.zkQuorum = zkQuorum;
    this.tableName = tableName;
    this.columns = columns;
  }

  public int load() {
    System.out.println("input : " + this.inputFolder + " output : " + this.outputFolder +
            " ZK Quorum : " + this.zkQuorum + " table : " + this.tableName + " columns : " + this.columns);
    if(this.inputFolder == null || this.outputFolder == null || this.zkQuorum == null || this.tableName == null || this.columns == null) {
      System.err.println("missing parameters");
      return -1;
    }
    Configuration config = new Configuration();
    config.set("hbase.zookeeper.quorum", this.zkQuorum);
    int exitCode = -1;
    try {
      exitCode = ToolRunner.run(config, new ImportTsv(), getJobArgs(this.inputFolder, this.outputFolder, this.tableName, this.columns));
      if (exitCode != 1) {
        System.out.println("successfully generate hfile");
      } else {
        System.out.println("failed generate hfile");
      }
    }catch(Exception e) {
      e.printStackTrace();
    }
    return exitCode;
  }
  
  //private static final String IMPORT_COLUMNS = "-Dimporttsv.columns=HBASE_ROW_KEY,x:id,x:type,x:src";
  private static final String IMPORT_COLUMNS = "-Dimporttsv.columns=HBASE_ROW_KEY,";
  private static final String HFILE_OUTPUT_KEY = "-Dimporttsv.bulk.output=";

  /**
   * @param inputFolder  eg. /user/dmp/<project>/tsv/<date>
   * @param outputFolder eg. /user/dmp/<project>/hfile/<date>
   * @param tableName eg. dmf
   * @param importColumns eg. x:id,x:type,x:src or d:max
   * @return
   */
  private static String[] getJobArgs(String inputFolder, String outputFolder, String tableName, String importColumns) {
    return new String[] {
        IMPORT_COLUMNS + importColumns,
        HFILE_OUTPUT_KEY + outputFolder,
        tableName,
        inputFolder
    };
  }
  
}
