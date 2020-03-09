package com.github.alekseyburger.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class HDFSOperation {
    protected String nameServerPort;
    protected Configuration conf;
    protected FileSystem fs;

    private static final Logger logger = LoggerFactory.getLogger(HDFSOperation.class);

    public HDFSOperation(String nameServerPort){
        this.nameServerPort = nameServerPort;
        this.init();
    }

    protected void init(){
        conf = new Configuration();

//        conf.addResource(new Path(nameServerPort + "/core-site.xml"));
//        conf.addResource(new Path(nameServerPort + "/hdfs-site.xml"));

        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        conf.set("fs.defaultFS",nameServerPort);

        try {
            fs = FileSystem.get(conf);
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
    }

    public void write(String hdfsPath, Iterable<String> array) {
        Path path = new Path(hdfsPath);
        try{
            //delete file if exist
            if(fs.exists(path)){
                logger.trace("Delete file {}", hdfsPath);
                fs.delete(path, true);
            }

            logger.trace("Create new file {}", hdfsPath);

            //create file and write content to file
            FSDataOutputStream fos = fs.create(path);
            for(String content : array ) {
                fos.writeUTF(content);
            }
            fos.close();
        }catch(IOException ex){
            logger.error(ex.getMessage());
        }
    }

    public String read(String hdfsPath) {
        String content = null;
        Path path = new Path(hdfsPath);
        try{
            //create file and write content to file
            FSDataInputStream fis = fs.open(path);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fis.getWrappedStream()));
            String s;
            Integer idx = 0;
            while((s = reader.readLine()) != null){
                content += idx.toString() + ":" + s;
                idx++;
            }
            reader.close();
            fis.close();
        }catch(IOException ex){
            logger.error(ex.getMessage());
        }
        return content;
    }

    public void copyFromLocal(String localPath, String hdfsPath){
        Path srcPath = new Path(localPath);
        Path destPath = new Path(hdfsPath);

        try{
            //check is path exist
            if(!fs.exists(destPath)){
                logger.error("No such destination " + destPath);
                return;
            }

            fs.copyFromLocalFile(srcPath, destPath);
        }catch(IOException ex){
            logger.error(ex.getMessage());
        }
    }

    public void delete(String hdfsPath) {
        Path path = new Path(hdfsPath);
        try{
            fs.delete(path, true);
        }catch(IOException ex){
            logger.error(ex.getMessage());
        }
    }
}