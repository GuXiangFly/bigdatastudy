package cn.guxiangfly.flinkstudy.scalacode.streaming.customSource

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object StreamingDemoWithNoParallelSourceScala {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val  text = env.addSource(new NoParallelSourceScala)
    val  mapData = text.map(line=>{
      println("接收到数据:"+line)
      line
    })

    val sum = mapData.timeWindowAll(Time.seconds(2)).sum(0)

    sum.print().setParallelism(1)
    env.execute("StreamingDemoWithNoParallelSourceScala")
  }
}
