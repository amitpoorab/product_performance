package spark_scala.merchant_page
import org.json4s.{DefaultFormats, Formats}
import org.scalatest.FunSuite
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}

import spark_scala.{AbstractsJob, Common}


class MerchantPageRankingsTest extends FunSuite{

test("test merchant page product rankings"){

	val (sc, sqlContext) = Common.getSparkContext("merchantProductPage", local=true)

	val offerEventsFilePath = getClass.getResource("/offerEvents.tsv").getFile

	// val offerEvents = loadEventsData(sqlcontext: SQLContext, path)
	val schema = StructType(Seq(
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
						.schema(schema)
						.csv(offerEventsFilePath)

	offerEvents.show()
}

}