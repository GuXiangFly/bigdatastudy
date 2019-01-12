package cn.guxiangfly.youtubesyllabus.etl;

import cn.guxiangfly.youtubesyllabus.utils.ETLUtil;
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

    private Text result = new Text();
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String etlString  = ETLUtil.oriString2ETLString(value.toString());
        if (etlString != null) {
            result.set(etlString);
            context.write(NullWritable.get(),result);
        }

    }
}
