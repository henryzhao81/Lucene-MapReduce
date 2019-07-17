package com.openx.audience.xdi.lucence.mapred;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.CompressionTools;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Version;

public class IndexBuilderReducer extends Reducer<Text, Text, Text, Text> {

	private static final String localIndexPath = "/var/tmp/dmp/index/";
	private static final String hdfsURL = "hdfs://xvaic-sg-36.xv.dc.openx.org:8020";//PROD
	//private static final String hdfsURL = "hdfs://xvaa-i35.xv.dc.openx.org:8020";//QA
	//private static final String hdfsURL = "hdfs://10.229.79.14:9000";
	private static final String hdfsPathBase = "/user/dmp/xdi/index/";
	private int taskId = -1;
	private IndexWriter writer;
	private static final String KEY_STORAGE_ID = "storageid";
	private static final String KEY_MAP_INFO = "mapinfo";
	private Text result = new Text();
//	Configuration conf = context.getConfiguration();
//	String param = conf.get("test");
	
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException, InterruptedException {
		this.taskId = context.getTaskAttemptID().getTaskID().getId();
		String jobId = context.getJobID().toString();
		System.out.println("======== setup at task : " + this.taskId + " job id : " + jobId + " host : " + InetAddress.getLocalHost().getHostName());
		//Delete All under index folder 
		//use global variable
		try {
			File localIndex = new File(localIndexPath);//localIndexPath : /var/tmp/dmp/index/
			File temp = new File(localIndexPath + jobId);
			if(localIndex.exists()) {
				//create temp job id folder
				if(!temp.exists()) {
					System.out.println("======== job id file " + (localIndexPath + jobId) + " does not exist, delete all " + " at task : " + this.taskId);
					for(File each : localIndex.listFiles()) { //user/dmp/xid/index/part_x
						if(each.isDirectory()) {
							for(File eachSub : each.listFiles()) { //user/dmp/xid/index/part_x/*
								System.out.println("delete exist file : " + eachSub.getAbsolutePath());
								if(eachSub.isFile()) {
									eachSub.delete();
								}
							}
						}
					}
					if(!temp.exists()) {
						System.out.println("======== create job id file " + (localIndexPath + jobId) + " at task : " + this.taskId);
						temp.createNewFile();
					}
				} else {
					System.out.println("======== job id file " + (localIndexPath + jobId) + " existed no need to delete all " + " at task : " + this.taskId);
				}
			} else {
				localIndex.mkdirs();
				if(!temp.exists()) {
					System.out.println("======== create job id file " + (localIndexPath + jobId) + " at task : " + this.taskId);
					temp.createNewFile();
				}
			}
		}catch(Exception ex) {
			System.out.println("Failed to delete file : " + ex.getMessage());
			ex.printStackTrace();
		}
		//wait for sec
		try {
			System.out.println("======== after delete all sleep 10s at task : " + this.taskId + " - " + System.currentTimeMillis());
			Thread.currentThread().sleep(10000);
		}catch(Exception ex) {
			ex.printStackTrace();
		}
		String pathName = localIndexPath + "part_" + this.taskId;
		File eachLocalIndex = new File(pathName);
		if(eachLocalIndex.exists()) {
			if(eachLocalIndex.isDirectory()) {
				for(File f : eachLocalIndex.listFiles()) {
					System.out.println("delete exist file : " + f.getAbsolutePath());
					f.delete();
				}
			}
		} else {
			eachLocalIndex.mkdirs();
		}
		//copy remote index file to local
		String date = context.getConfiguration().get("previous_date");
		String hdfsPath = hdfsPathBase;
		if(date != null && date.length() > 0)
			hdfsPath = hdfsPath + date + "/";
		FileSystem remoteFileSystem = null;
		try {
			URI hdfsNamenode = new URI(hdfsURL);
			remoteFileSystem = FileSystem.get(hdfsNamenode, new Configuration());		
			Path source = new Path(hdfsPath + "part_" + this.taskId, "*");
			final FileStatus[] allFiles = remoteFileSystem.globStatus(source);
			for(FileStatus eachFile : allFiles) {
				Path remotePath = eachFile.getPath();
				if(remotePath != null) {
					System.out.println("-- copy remote index file path : " + remotePath.toString() + " to local folder " + pathName);
					remoteFileSystem.copyToLocalFile(remotePath, new Path(pathName));
				} else {
					System.out.println("-- remote index file path is null");
				}
			}
			
		}catch(Exception ex) {
			System.out.println("Failed to copy to local : " + hdfsURL + ":" + hdfsPath + " | " + ex.getMessage());
		}
		System.out.println("======== setup FSDirectory open path : " + pathName + " at " + this.taskId);
        //create index writer		
		Directory dir = NIOFSDirectory.open(new File(pathName));
		Analyzer analyzer = new WhitespaceAnalyzer();
		IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_4_10_3, analyzer);//TODO upgrade to LUCENE_4_10_4
		iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
		writer = new IndexWriter(dir, iwc);
		if(writer == null)
			throw new InterruptedException("Failed to create index writer");
		writer.forceMerge(1);
		System.out.println("======== setup at task : " + this.taskId + " completed");
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int index = 0;
		String[] latestArray = null;
		long lastest = -1;
		for(Text value : values) {
			String[] info = value.toString().split("&");
			long time = Long.parseLong(info[0]);
			if(time >= lastest)
				latestArray = info;
			index++;
		}
		if(latestArray != null && latestArray.length > 2) {
			Document doc = new Document();
			String storageId = key.toString();
			doc.add(new StringField(KEY_STORAGE_ID, storageId, Field.Store.YES));
			String mapInfo = latestArray[1] + "&" + latestArray[2]; // mid + browserType
			doc.add(new StoredField(KEY_MAP_INFO, CompressionTools.compress(mapInfo.getBytes())));
			this.writer.updateDocument(new Term(KEY_STORAGE_ID, storageId), doc);
		}
		result.set(Integer.toString(index));
		context.write(key, result);
	}
	
	@Override
	protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException, InterruptedException {
		System.out.println("======== shut down at task : " + this.taskId);
		String pathName = localIndexPath + "part_" + this.taskId + "/";
		File localPath = new File(pathName);
		if(writer != null) {
			if(localPath.exists()) {
				System.out.println("======== optimized : " + this.taskId);
			    writer.forceMerge(1);
			}
			System.out.println("======== commit : " + this.taskId);
			//writer.close();
			writer.commit();
		}
		if(!localPath.exists())
			return;
		System.out.println("======== prepare copy to  remote : " + this.taskId);
		String date = context.getConfiguration().get("current_date");
		String hdfsPath = hdfsPathBase;
		if(date != null && date.length() > 0)
			hdfsPath = hdfsPath + date + "/";
		boolean copySuccess = true;	
		FileSystem remoteFileSystem = null;
		try {
			URI hdfsNamenode = new URI(hdfsURL);
			remoteFileSystem = FileSystem.get(hdfsNamenode, new Configuration());
			Path remotePath = new Path(hdfsPath + "part_" + this.taskId);
			if(!remoteFileSystem.exists(remotePath)) {
				System.out.println("======== create remote path : " + hdfsPath + "part_" + this.taskId + " at : " + this.taskId);
				remoteFileSystem.mkdirs(remotePath);
			}
			File[] files = localPath.listFiles(); 
			//delete crc file
			for(File file : files) {
				String absolutePath = file.getAbsolutePath();
				if(absolutePath.endsWith(".crc")) {
					System.out.println("delete crc file : " + absolutePath);
					file.delete();
				}
			}
			files = localPath.listFiles();
			for(File file : files) {
				String absolutePath = file.getAbsolutePath();
				System.out.println("======== copy local file " + absolutePath + " to " + (hdfsPath + "part_" + this.taskId) + " at : " + this.taskId);
				remoteFileSystem.copyFromLocalFile(false, true, new Path(absolutePath), remotePath);
			}
		}catch(Exception ex) {
			System.out.println("Failed to copy to HDFS : " + hdfsURL + ":" + hdfsPath + " | " + ex.getMessage());
			copySuccess = false;
		} 
		if(copySuccess) {
			System.out.println("Copy to HDFS successed, remove current index files : " + pathName);
			try {
				if(remoteFileSystem != null)
					remoteFileSystem.close();
				File file = new File(pathName);
				if(file.isDirectory()) {
					for(File f : file.listFiles()) {
						System.out.println("delete created file : " + f.getAbsolutePath());
						f.delete();
					}
					System.out.println("delete created folder : " + file.getAbsolutePath());
					file.delete();
				}
			} catch(Exception ex) {
                ex.printStackTrace();
			}
		}
		//remove previous temp job files, not current one.
		System.out.println("==> last step remove previous temp job files");
		String jobId = context.getJobID().toString();
		File jobTemp = new File(localIndexPath);
		if(jobTemp.exists() && jobTemp.isDirectory()) {
			for(File each : jobTemp.listFiles()) {
				if(each.isFile()) {
					String fileName = each.getName();
					StringBuffer buffer = new StringBuffer();
					buffer.append(fileName);
					if(!fileName.equals(jobId)) {
						each.delete();
						buffer.append("-");
						buffer.append("deleted");
					} else {
						buffer.append("-");
						buffer.append("keep");
					}
				    System.out.println(" each job file " + buffer.toString() + " at task : " + this.taskId);
				}
			}
		}
	}
}
