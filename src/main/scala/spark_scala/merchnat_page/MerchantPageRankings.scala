package spark_scala.merchnat_page

import java.time.{Instant}
import org.rogach.scallop.ScallopConf


import org.apache.spark.sql.{DataFrame, SQLContext}
import spark_scala.{AbstractsJob, Common}
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{coalesce, col, explode, lit, lower, max, row_number, when, udf}


case class MerchantPageProductRankingsParams( startTs: Instant,
										endTs: Instant,
										eventDF: DataFrame )

case class MerchantPageProductRankingsResults (output: DataFrame)

object MerchantPageProductRankings extends AbstractsJob[MerchantPageProductRankingsParams, 
														MerchantPageProductRankingsResults]{



def run(params: MerchantPageProductRankingsParams): MerchantPageProductRankingsResults = {

	MerchantPageProductRankingsResults(params.eventDF)
}

val schema = StructType(Seq(
			StructField("core", StructType(Seq(
				StructField("productID", StringType, true),
				StructField("productID", StringType, true),
				StructField("productID", StringType, true),
				StructField("productID", StringType, true)	
			)), true)
	)
)

class Parser(arguments: Seq[String]) extends ScallopConf(arguments){
	val startTs = opt[String](required = true, descr = "Strat time stamp")
	val endTs = opt[String](required = true, descr = "end time stamp")
	val eventDataPath = opt[String](required = true, descr = "event data path")
	verify()
}

def main(args: Array[String]) = {
	val parsedArguments = new Parser(args)
	val (sc, sqlContext) = Common.getSparkContext("merchantProductPage")
	// import sc.implicits._


	val startTs = Instant.parse(parsedArguments.startTs())	
	val endTs = Instant.parse(parsedArguments.endTs())	

	val eventDF = sqlContext.read.json(parsedArguments.eventDataPath())


	val result = run(MerchantPageProductRankingsParams(startTs
													   ,endTs	
													   ,eventDF))
}



}