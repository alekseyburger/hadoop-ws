package com.github.alekseyburger.hdfsTest;

import com.github.alekseyburger.hdfs.HDFSOperation;
import org.apache.log4j.helpers.Loader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


public class writeExample {

    private static final Logger logger = LoggerFactory.getLogger(HDFSOperation.class);

    public static void main(String[] argv) {

        if (argv.length <= 1) {
            logger.error("Please define hdfs name server port like hdfs://name:9000");
            System.exit(1);
        }

        InputCollection<String> inpGen = new InputCollection();

        HDFSOperation hDFSOperation = new HDFSOperation(argv[1]);

        String HdfsFilePath = "/test/testing.txt";

        logger.info("Start to write on " + argv[1] + " file " + HdfsFilePath);
        hDFSOperation.write(HdfsFilePath, inpGen);

        //System.out.println(hDFSOperation.read("hdfs://wdesktop:9000/test/testing.txt"));

        //hDFSOperation.copyFromLocal("/home/alekseyb/workspace-java/hadoopio/local.txt", "hdfs://wdesktop:9000/test/testing.txt");
        logger.info("Done.");
    }
}

