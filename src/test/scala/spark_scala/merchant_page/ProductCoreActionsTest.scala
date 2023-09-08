package spark_scala.merchant_page

import java.time.Instant
import org.json4s.{DefaultFormats, Formats}
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.scalatest.FunSuite
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import spark_scala.{AbstractsJob, Common}
import spark_scala.golden_dataset.EventActions
import spark_scala.merchnat_page._

class ProductCoreActionsTest extends FunSuite{

test("test merchant page product rankings"){

	val (sc, sqlContext) = Common.getSparkContext("merchantProductPage", local=true)

	val offerEventsFilePath = getClass.getResource("/productEvents.tsv").getFile


	val offerEvents = sqlContext.read
						.option("delimiter", "\t")
						.option("header", false)
						.schema(EventActions.eventSchema)
						.csv(offerEventsFilePath)

	val startTs = Instant.parse("2023-01-01T00:00:00Z")
	val endTs = Instant.parse("2023-01-01T23:00:00Z")

	val results = ProductCoreActions.run(ProductCoreActionsParams(sc, sqlContext, startTs, endTs, offerEvents))
	results.output.show(false)
}

}