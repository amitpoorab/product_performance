package spark_scala

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.SparkContext 

object Common {

def getSparkContext(applicationName: String, local: Boolean = false): (SparkContext, SQLContext, SparkSession) = {

	val sparkSessionBuilder = SparkSession.builder().appName(applicationName)
	if (local) {
		sparkSessionBuilder.master("local")
		sparkSessionBuilder.config("spark.driver.allowMultipleContext","true")
	}

	val sparkSession = sparkSessionBuilder.getOrCreate()
	sparkSession.sparkContext.setLogLevel("ERROR")
	sparkSession.sparkContext.hadoopConfiguration.set("fs.s3.canned.acl","BucketOwnerFullControl")
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.connection.maximum","5000")
    (sparkSession.sparkContext, sparkSession.sqlContext, sparkSession)

	}

	case class Product (
											 uuid: String,
											 name: String,
											 category: String  )

	case class AggregatedEvents (
																experimentSlice: String,
																productID: String,
																platform: String ,
																rank: String ,
																renderedImpressions: String ,
																//OnlineClick: Option[BigInt] ,
																onlineClick: Option[Int] ,
																onlineOrder: Option[Int]
															)
	org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this)

}

/*
common schemas
*/


