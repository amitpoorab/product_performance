package spark_scala.merchant_page

import java.time.{Instant}
import org.rogach.scallop.ScallopConf
import spark_scala.{AbstractsJob, Common}

case class MerchantPageProductRankingsParams( startTs: Instant,
										endTs: Instant,
										eventDF: DataFrame )

case class MerchantPageProductRankingsResults (output: DataFrame)

object MerchantPageProductRankings extends AbstractsJob[MerchantPageProductRankingsParams, 
														MerchantPageProductRankingsResults]{



def run(params: MerchantPageProductRankingsParams): MerchantPageProductRankingsResults = {

	MerchantPageProductRankingsResults(params.eventDF)
}

class Parser(arguments: Seq[String]) extends ScallopConf(arguments){
	val startTs = opt[String](required = true, descr = "Start timestamp")
	val endTs = opt[String](required = true, descr = "end timestamp")
	val eventDataPath = opt[String](required = true, descr = "event data path")
	verify()
}

def main(args: Array[String]) = {
	val parsedArguments = new Parser(args)
	val (sc, sqlContext, sparkSession) = Common.getSparkContext("merchantProductPage")
	// import sc.implicits._


	val startTs = Instant.parse(parsedArguments.startTs())	
	val endTs = Instant.parse(parsedArguments.endTs())	

	val eventDF = sqlContext.read.json(parsedArguments.eventDataPath())


	val result = run(MerchantPageProductRankingsParams(startTs
													   ,endTs	
													   ,eventDF))
}



}