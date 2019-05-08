package cn.guxiangfly.flinkstudy.scalacode.streaming

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object StreamingFromCollectionScala {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    val data = List(10,15,20)
    import org.apache.flink.api.scala._

    val text = env.fromCollection(data)
    val num = text.map (_ + 1).setParallelism(6)
    num.print().setParallelism(1)
    env.execute("StreamingFromCollectionScala")
  }
}