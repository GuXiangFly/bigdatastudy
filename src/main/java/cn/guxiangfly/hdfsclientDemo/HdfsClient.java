package cn.guxiangfly.hdfsclientDemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

public class HdfsClient {

    public static void main(String[] args) throws Exception {
	// write your code here
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","http://hadoop102:9000");

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"),configuration,"guxiang");
        fs.copyFromLocalFile(new Path("d://2.jpg"),new Path("/2.jpg"));
        fs.close();
        System.out.println("over");
    }

    @Test
    public void  initHDFS() throws Exception{
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","http://hadoop102:9000");

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"),configuration,"guxiang");
        fs.copyToLocalFile(false,new Path("/2.jpg"),new Path("d:/22.jpg"),true);
        fs.close();
        System.out.println("over");
    }

    @Test
    public void putFileToHdfs() throws Exception{
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","http://hadoop102:9000");

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"),configuration,"guxiang");
        FileInputStream fileInputStream = new FileInputStream(new File("d://2.jpg"));
        FSDataOutputStream fsDataOutputStream = fs.create(new Path("/2ByIO.jpg"));

        IOUtils.copyBytes(fileInputStream,fsDataOutputStream,configuration);
        IOUtils.closeStream(fileInputStream);
        IOUtils.closeStream(fsDataOutputStream);
    }
}
