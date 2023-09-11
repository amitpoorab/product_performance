package spark_scala.merchant_page

import org.scalatest.FunSuite
import spark_scala.Common
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions._

class MerchantPageRankingsTest extends FunSuite {

	test("test merchant page product rankings") {

		val (sc, sqlContext, sparkSession) = Common.getSparkContext("merchantProductPage", local = true)

		val productFilePath = getClass.getResource("/product.json").getFile
		val aggregatedEventsPath = getClass.getResource("/aggregatedEvents.json").getFile

		import sqlContext.implicits._


		val productDS = sqlContext.read.json(productFilePath)
			.as[Common.Product]

		val schema = Encoders.product[Common.AggregatedEvents].schema
		val aggregatedEventsDS = sqlContext.read
			.schema(schema)
			.json(aggregatedEventsPath)
			.as[Common.AggregatedEvents]

		val joinProductAndEvents = productDS.as("products").join(aggregatedEventsDS.as("aggEvents"),
			productDS("uuid") === aggregatedEventsDS("productID")
		)
			.select("category", "productID", "rank", "onlineClick")

		val joinProductAndEventsWithCorrectedClicks = joinProductAndEvents
			.withColumn("onlineClick",
				when(col("onlineClick").isNull,
					lit(0)).otherwise(col("onlineClick")))
		joinProductAndEventsWithCorrectedClicks.show(false)

		val joinProductAndEventsRdd = joinProductAndEventsWithCorrectedClicks.rdd
		val joinProductAndEventsLst = joinProductAndEventsRdd.collect()

		val actualResultMap = joinProductAndEventsLst.map {
			row =>
				val category = row.getAs[String]("category")
				val rank = row.getAs[String]("rank")
				val onlineClick = row.getAs[Int]("onlineClick")
				val value = s"$category$rank$onlineClick"

				(row.getAs[String]("productID"), value)
		}.toMap

		val expectedLst = Seq(("home", "product1", 3, 2), ("home", "product2", 2, 2), ("home", "product3", 3, 1)
			, ("work", "product4", 1, 1), ("home", "product5", 2, 0))

		val expectedMap = expectedLst.map {
			x =>
				val category = x._1
				val rank = x._3
				val click = x._4

				(x._2, s"$category$rank$click")
		}.toMap

		actualResultMap.foreach { case (key, value) =>
			val expectedVal = expectedMap.getOrElse(key, " ")
			assert(value.equals(expectedVal))
		}

	}
}
