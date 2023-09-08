package spark_scala.merchnat_page

import java.time.{Instant}
import org.rogach.scallop.ScallopConf
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{coalesce, col, explode, lit, lower, max, row_number, when, udf}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.SparkContext 
import org.apache.spark.sql.{DataFrame, SQLContext}
import spark_scala.{AbstractsJob, Common}
import spark_scala.golden_dataset
import spark_scala.utils.StringUtils.parseDateFromString

import java.time.{Instant, ZoneOffset}
import scala.util.Try


case class ProductCoreActionsParams( sc: SparkContext,
									 sqlContext: SQLContext, 
										startTs: Instant,
										endTs: Instant,
										eventDF: DataFrame )

case class ProductCoreActionsResults (output: DataFrame)

object ProductCoreActions extends AbstractsJob[ProductCoreActionsParams, 
														ProductCoreActionsResults]{

	def filterByTime(data: DataFrame, startTs: Instant, endTs: Instant): DataFrame = {

		def inRange(d: String) : Boolean = {
			val eventTs = parseDateFromString(d).toInstant(ZoneOffset.UTC)
			eventTs.isAfter(startTs) && (eventTs.isBefore(endTs) | eventTs.equals(endTs))
		}
		val inRangeUDF = udf(inRange _)

		data.withColumn("eventTs", coalesce(col("coreActionTimestamp"),col("renderedTimestamp")))
			.where(col("eventTs").isNotNull)
			.where(inRangeUDF(col("eventTs")))
	}
	def aggregateImpressions(coreActionData: DataFrame): DataFrame = {
		coreActionData.groupBy("experimentSlice","productID", "platform", "rank")
			.agg(countDistinct("eventInstanceUuid").as("renderedImpressions"))
	}

	def aggregateCoreActions(coreActionData: DataFrame): DataFrame = {
		coreActionData.groupBy("experimentSlice","productID", "platform", "rank","coreAction")
			.agg(countDistinct("eventInstanceUuid", "coreActionTimestamp").as("coreActionCount"))
			.groupBy("experimentSlice","productID", "platform", "rank")
			.pivot("coreAction")
			.agg(sum(col("coreActionCount")))
	}

	def joinImpsAndCoreActions(impressionsAgg: DataFrame, coreActionsAgg: DataFrame): DataFrame = {
			impressionsAgg.as("imps").join(coreActionsAgg.as("ca"),
				impressionsAgg("experimentSlice") === coreActionsAgg("experimentSlice") &&
				impressionsAgg("productID") === coreActionsAgg("productID") &&
				impressionsAgg("platform") === coreActionsAgg("platform") &&
				impressionsAgg("rank") === coreActionsAgg("rank")
			)
			.select("imps.experimentSlice",
				"imps.productID",
				"imps.platform",
				"imps.rank",
				"imps.renderedImpressions",
				"ca.OnlineClick",
				"ca.OnlineOrder"
				)
	}

	def run(params: ProductCoreActionsParams): ProductCoreActionsResults = {

		val coreActionsFilteredByTime = filterByTime(params.eventDF, params.startTs, params.endTs)
		val aggregatedCoreAction = aggregateCoreActions(coreActionsFilteredByTime)
		val aggregatedImpressions = aggregateImpressions(coreActionsFilteredByTime)
		val joinedImpsAndAgg = joinImpsAndCoreActions(aggregatedImpressions, aggregatedCoreAction)		
		ProductCoreActionsResults(joinedImpsAndAgg)
	}
	

class Parser(arguments: Seq[String]) extends ScallopConf(arguments){
	val startTs = opt[String](required = true, descr = "Start timestamp")
	val endTs = opt[String](required = true, descr = "end timestamp")
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


	val result = run(ProductCoreActionsParams( sc 
											  , sqlContext
											  , startTs
											  , endTs	
											  , eventDF))
	}



}