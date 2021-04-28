import org.apache.flink.api.scala._

object WordCountScalaBatch {
  def main(args: Array[String]): Unit = {

    val inputPath = "E:\\hadoop_res\\input\\a.txt"
    val outputPath = "E:\\hadoop_res\\output2"

    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val text: DataSet[String] = environment.readTextFile(inputPath)
    text
      .flatMap(_.split("\\s+"))
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .setParallelism(1)
      .writeAsCsv(outputPath, "\n", ",")


    //setParallelism(1)很多算子后面都可以调用
    environment.execute("job name")

  }

}
