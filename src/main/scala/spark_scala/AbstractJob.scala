package spark_scala

import org.rogach.scallop.ScallopConf

abstract class AbstractsJob[Parameters, RunResult]{
	def run(params: Parameters): RunResult

	class Parser (arguments: Seq[String]) extends ScallopConf(arguments)

	def main(args: Array[String]): Unit
}