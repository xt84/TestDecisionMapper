package xt84.info.decisionmapper.csvtrs

object Main extends App {
  val KEYS = Map(
    "data"    -> "Path to data",
    "rules"   -> "Path to transformation rules configuration",
    "report"  -> "Path to report file"
  )

  override def main(args: Array[String]): Unit = {
    if ((args.length == 0) || ((args.length % KEYS.size) > 0) || (args.length < 3)) {
      println(usage())
    } else {
      //TODO: job init code
    }
  }

  def usage(): String = KEYS.map(kv => kv._1 -> s"--${kv._1} ${kv._2}").values.mkString(" ")

  def parseArguments(argsMap: Map[String, String] = Map(), args: List[String]): Map[String, String] = {
    def isKey(arg: String) = arg.startsWith("--")
    args match {
      case Nil => argsMap
      case arg :: args if isKey(arg) => parseArguments(
        argsMap ++ Map(arg.replaceAll("--", "") -> args.head),
        args.tail
      )
    }
  }
}
