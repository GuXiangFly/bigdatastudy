package cn.guxiangfly.hdfsclientDemo.flowsort;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class FlowSortReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

//	7335	110349	117684  13502468823 ----13502468823  7335	110349	117684
//	7335	110349	117684  13502468824
	@Override
	protected void reduce(FlowBean key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

			context.write(values.iterator().next(),key);

	}
}
