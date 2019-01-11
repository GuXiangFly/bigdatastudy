package cn.guxiangfly.hdfsclientDemo.order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import java.security.Key;

public class OrderPatitioner extends Partitioner<OrderBean, NullWritable> {

    @Override
    public int getPartition(OrderBean key, NullWritable value, int numberPartition) {
        return (key.getOrderId().hashCode() & Integer.MAX_VALUE) % numberPartition;
    }
}