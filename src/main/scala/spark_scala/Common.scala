package spark_scala

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.SparkContext 

object Common {

def getSparkContext(applicationName: String, local: Boolean = false): (SparkContext, SQLContext) = {

	val sparkSessionBuilder = SparkSession.builder().appName(applicationName)
	if (local) {
		sparkSessionBuilder.master("local")
		sparkSessionBuilder.config("spark.driver.allowMultipleContext","true")
	}

	val sparkSession = sparkSessionBuilder.getOrCreate()
	sparkSession.sparkContext.setLogLevel("ERROR")
	sparkSession.sparkContext.hadoopConfiguration.set("fs.s3.canned.acl","BucketOwnerFullControl")
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.connection.maximum","5000")
    (sparkSession.sparkContext, sparkSession.sqlContext)	

	}
}