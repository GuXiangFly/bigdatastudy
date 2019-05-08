package cn.guxiangfly.hdfsclientDemo.mr;

import java.io.IOException;

import org.apache.hadoop.fs.shell.Count;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

    //atguigu 1 atguigu 1
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
                          Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable count : values) {
            sum +=count.get();
        }
        context.write(key,new IntWritable(sum));
    }
}
