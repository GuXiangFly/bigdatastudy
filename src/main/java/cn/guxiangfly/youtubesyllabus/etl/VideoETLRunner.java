package cn.guxiangfly.youtubesyllabus.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * VideoETLRunner
 *
 * @author guxiang
 * @date 2019/1/9
 */
public class VideoETLRunner implements Tool {

    private Configuration conf = null;

    /**
     *
     * job 在运行的时候 回调的方法
     * @param args
     * @return
     * @throws Exception
     */
    public int run(String[] args) throws Exception {

        conf.set("inpath",args[0]);
        conf.set("outpath",args[1]);

        Job job = Job.getInstance(conf);
        job.setJarByClass(VideoETLRunner.class);
        job.setMapperClass(VideoETLMapper.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setNumReduceTasks(0);
        this.initJobOutputPath(job);
        this.initJobInputPath(job);

        return job.waitForCompletion(true)?1:-1;
    }


    private void initJobOutputPath(Job job) throws IOException {
        Configuration conf = job.getConfiguration();
        String outPathString = conf.get("outpath");
        FileSystem fs= FileSystem.get(conf);
        Path outPath = new Path(outPathString);
        if (fs.exists(outPath)){
            fs.delete(outPath,true);
        }
        FileOutputFormat.setOutputPath(job,outPath);
    }

    private void initJobInputPath(Job job) throws IOException{
        Configuration conf = job.getConfiguration();
        String inPathString = conf.get("inpath");

        Path inPath = new Path(inPathString);
        FileSystem fs= FileSystem.get(conf);
        if (fs.exists(inPath)){
            fs.delete(inPath,true);
        }else {
            throw  new IOException("您传输的输入路径不合法");
        }
    }

    /**
     * 这个configuration 是在job提交给集群的时候 集群内部维护了一个conf   集群中的configuration会传入job中
     * @param configuration
     */
    public void setConf(Configuration configuration) {
        this.conf = configuration;
    }

    public Configuration getConf() {
        return null;
    }


    public static void main(String[] args) throws Exception{
        VideoETLRunner videoETLRunner = new VideoETLRunner();
        int resultCode = ToolRunner.run(videoETLRunner,args);
        System.exit(resultCode);
    }
}
