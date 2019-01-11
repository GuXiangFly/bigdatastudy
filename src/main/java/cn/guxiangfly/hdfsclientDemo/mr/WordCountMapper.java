package cn.guxiangfly.hdfsclientDemo.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * WordCountMapper
 *    
 * @author guxiang
 * @date 2018/8/27
 */
public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    public Text k = new Text();

    public IntWritable v = new IntWritable(1);

    @Override
    protected  void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] split = line.split(" ");

        for (String word : split) {
            k.set(word);
            context.write(k,v);
        }
    }
}
