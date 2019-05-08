package cn.guxiangfly.flinkstudy.scalacode.streaming.customSource

import org.apache.flink.streaming.api.functions.source.SourceFunction

class NoParallelSourceScala extends SourceFunction[Long]{

  var count = 0L
  @volatile  var isRunning = true
  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while (isRunning){
      ctx.collect(count)
      count+=1
      Thread.sleep(1000)
    }
  }

  /**
    * 任务停止的时候  会调用的方法
    */
  override def cancel(): Unit = {
    isRunning= false
  }
}
