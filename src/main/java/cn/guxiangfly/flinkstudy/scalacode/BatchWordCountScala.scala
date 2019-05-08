package cn.guxiangfly.flinkstudy.scalacode

import java.util.concurrent.ExecutionException

import org.apache.flink.api.scala.ExecutionEnvironment

object BatchWordCountScala {
  def main(args: Array[String]): Unit = {
    val inputPath = "E:\\crawlerdata\\sinaappletxt"
    val outPath = "E:\\crawlerdata\\sinaappletxtresultcount"
    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.readTextFile(inputPath)

    //引入隐式转换
    import org.apache.flink.api.scala._

    val counts = text.flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_,1))
      .groupBy(0)
      .sum(1)
    counts.writeAsCsv(outPath,"\n"," ").setParallelism(1)
    env.execute("batch word count")

  }
}
