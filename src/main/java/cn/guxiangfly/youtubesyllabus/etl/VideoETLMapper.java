package cn.guxiangfly.youtubesyllabus.etl;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

/**
 * VideoETLMapper
 *
 * @author guxiang
 * @date 2019/1/9
 */
public class VideoETLMapper  extends Mapper<Object,Text,NullWritable,Text> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

    }
}
