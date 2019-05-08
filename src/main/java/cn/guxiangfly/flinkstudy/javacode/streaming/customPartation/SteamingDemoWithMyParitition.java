package cn.guxiangfly.flinkstudy.javacode.streaming.customPartation;
import cn.guxiangfly.flinkstudy.javacode.MyNoParalleSource.MyNoParalleSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SteamingDemoWithMyParitition {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<Long> text = env.addSource(new MyNoParalleSource());

        // 先进行一次 算子操作
        DataStream<Tuple1<Long>> tupleData = text.map(new MapFunction<Long, Tuple1<Long>>() {
            @Override
            public Tuple1<Long> map(Long value) throws Exception {
                return new Tuple1<>(value);
            }
        });
        //将第一次算子操作后的数据进行分区   后面一个参数0 是指   使用数据源的第0个元素进行分区
        DataStream<Tuple1<Long>> partitionData= tupleData.partitionCustom(new MyPartition(), 0);

        // 再进行一次 算子操作
        DataStream<Long> result =(SingleOutputStreamOperator<Long>) partitionData.map(new MapFunction<Tuple1<Long>, Long>() {
            @Override
            public Long map(Tuple1<Long> value) throws Exception {
                System.out.println("当前线程id：" + Thread.currentThread().getId() + ",value: " + value);
                return value.getField(0);
            }
        });
        result.print().setParallelism(1);

        env.execute("SteamingDemoWithMyParitition");
    }
}
