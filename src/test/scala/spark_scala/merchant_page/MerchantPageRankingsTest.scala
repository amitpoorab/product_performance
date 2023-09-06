package spark_scala.merchant_page

import java.time.{Instant}
import org.json4s.{DefaultFormats, Formats}
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.scalatest.FunSuite
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}

import spark_scala.{AbstractsJob, Common}
import spark_scala.golden_dataset.EventActions

class MerchantPageRankingsTest extends FunSuite{

test("test merchant page product rankings"){

	val (sc, sqlContext) = Common.getSparkContext("merchantProductPage", local=true)

	val offerEventsFilePath = getClass.getResource("/productEvents.tsv").getFile

	// val offerEvents = loadEventsData(sqlcontext: SQLContext, path)
	
	val rawEventschema = StructType(Seq(
			StructField("core", StructType(Seq(
				StructField("productID", StringType, true),
				StructField("productID", StringType, true),
				StructField("productID", StringType, true),
				StructField("productID", StringType, true)	
			)), true)
	)
)

	val offerEvents = sqlContext.read
						.option("delimiter", "\t")
						.option("header", false)
						.schema(EventActions.eventSchema)
						.csv(offerEventsFilePath)

	offerEvents.show()
}

}