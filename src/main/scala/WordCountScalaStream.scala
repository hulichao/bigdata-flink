import org.apache.flink.streaming.api.scala._

object WordCountScalaStream {
  def main(args: Array[String]): Unit = {
    //处理流式数据
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val streamData: DataStream[String] = environment.socketTextStream("linux121", 7777)

    val out: DataStream[(String, Int)] = streamData
      .flatMap(_.split("\\s+"))
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    out.print()

    environment.execute("test stream")
  }

}
