package cn.bhfae

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object Tools {

	// 封装Request
	case class Req(token: String, visitorId: String, promotionId: String, reqTimestamp: String)

	// 封装Response
	case class Res(token: String, resultCode: String, prizeLevel: String)

	/**
	  * 根据指定的配置条件获取SparkSession
	  *
	  * @return 返回SparkSession
	  */
	def getSparkSession: SparkSession = {
		val spark: SparkSession = SparkSession
				.builder()
				.appName(s"${this.getClass.getSimpleName}")
				//        		.master("local[5]")
				.config("spark.some.config.option", "some-value")
				.getOrCreate()
		spark
	}

	/**
	  * 根据指定的编码方式读取文件
	  *
	  * @param sc   SparkContext
	  * @param path 读取文件的路径
	  * @return 返回读取到的文件
	  */
	def read(sc: SparkContext, path: String): RDD[String] = {
		sc.hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text])
				.map(p => new String(p._2.getBytes, 0, p._2.getLength, "GBK"))
	}

	/**
	  * 把指定编码格式的数据追加到数据库
	  *
	  * @param df 需要存储的时间
	  */
	def writeToMysql(df: DataFrame): Unit = {
		df.write
				.mode("append")
				.format("jdbc")
				.option("url", "jdbc:mysql://hadoop01:3306/lottery?useUnicode=true&characterEncoding=gbk")
				.option("user", "root")
				.option("password", "root")
				.option("dbtable", "lot")
				.save()
	}

}
