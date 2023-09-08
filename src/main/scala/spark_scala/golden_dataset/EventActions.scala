package spark_scala.golden_dataset

import java.time.{Instant}
import org.rogach.scallop.ScallopConf
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}

import spark_scala.{AbstractsJob, Common}

// // This file will read the raw files and will process and preaggregate all the data for subsequent jobs. 

case class EventActionsParams( startTs: Instant,
										endTs: Instant,
										eventDF: DataFrame )

case class EventActionsResults (output: DataFrame)


object EventActions extends AbstractsJob[EventActionsParams, 
														EventActionsResults]{


val eventSchema = StructType(Seq(
					StructField("timestamp",StringType,true),
					StructField("eventType",StringType,true),
					StructField("eventInstanceUuid",StringType,true),
					StructField("renderedTimestamp",StringType,true),
					StructField("coreActionEventInstanceUuid",StringType,true),
					StructField("coreActionTimestamp",StringType,true),
					StructField("platform",StringType,true),
					StructField("merchantUuid",StringType,true),
					StructField("productId",StringType,true),					
					StructField("experimentSlice",StringType,true),
	 				StructField("rank",StringType,true),
	  			StructField("coreAction",StringType,true)
					)
)

def run(params: EventActionsParams): EventActionsResults = {

	EventActionsResults(params.eventDF)
}

class Parser(arguments: Seq[String]) extends ScallopConf(arguments){
	val startTs = opt[String](required = true, descr = "Strat time stamp")
	val endTs = opt[String](required = true, descr = "end time stamp")
	val eventDataPath = opt[String](required = true, descr = "event data path")
	verify()
}

def main(args: Array[String]) = {
	val parsedArguments = new Parser(args)
	val (sc, sqlContext, sparkSession) = Common.getSparkContext("eventActions")
	// import sc.implicits._


	val startTs = Instant.parse(parsedArguments.startTs())	
	val endTs = Instant.parse(parsedArguments.endTs())	

	val eventDF = sqlContext.read.json(parsedArguments.eventDataPath())


	val result = run(EventActionsParams(startTs
										   ,endTs	
										   ,eventDF))
}


}