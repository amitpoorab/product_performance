package spark_scala.merchant_page

import java.time.Instant
import org.scalatest.FunSuite
import spark_scala.{AbstractsJob, Common}
import spark_scala.golden_dataset.EventActions
import spark_scala.merchnat_page._

class ProductCoreActionsTest extends FunSuite {

	test("test merchant page product rankings") {

		val (sc, sqlContext, sparkSession) = Common.getSparkContext("merchantProductPage", local = true)

		val offerEventsFilePath = getClass.getResource("/productEvents.tsv").getFile


		val offerEvents = sqlContext.read
			.option("delimiter", "\t")
			.option("header", false)
			.schema(EventActions.eventSchema)
			.csv(offerEventsFilePath)

		val startTs = Instant.parse("2023-01-01T00:00:00Z")
		val endTs = Instant.parse("2023-01-01T23:00:00Z")

		val results = ProductCoreActions.run(ProductCoreActionsParams(sc, sqlContext, startTs, endTs, offerEvents))

		val expectedLst = Seq(("control", "product31", "desktop", 3, 2, 2, null),
													("control", "product21", "desktop", 2, 2, 2, null),
													("control", "product21", "desktop", 3, 1, 1, null),
													("control", "product12", "desktop", 1, 2, 1, null),
													("control", "product12", "desktop", 2, 1, null, null))

		val expectedList = sc.parallelize(expectedLst).collect()
		val expectedMap = expectedList.map {
			x =>
				val category = x._1
				val rank = x._3
				val click = x._4

				(x._2, s"$category$rank$click")
		}.toMap

		results.output.show(false)
		/*
			TODO : need to add assert statement
		*/
	}
}