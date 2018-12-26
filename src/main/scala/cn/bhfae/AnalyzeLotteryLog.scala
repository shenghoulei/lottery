package cn.bhfae

import cn.bhfae.Tools.{Req, Res}
import com.alibaba.fastjson.JSON

object AnalyzeLotteryLog {

	def main(args: Array[String]) {
		//  参数校验
		if (args.length < 2) {
			System.err.println("Usage: inputPath outputPath")
			System.exit(1)
		}
		val inputPath = args(0)
		val outputPath = args(1)

		// 获取sparkSession，并引入隐式转换
		val spark = Tools.getSparkSession
		import spark.implicits._

		// 读取源数据
		val lines = Tools.read(spark.sparkContext, inputPath)

		// 过滤Request数据
		val filtedRequest = lines.filter(filterRequest)
		// 提取Request数据
		val extractedRequest = filtedRequest.map(extractRequest)
		// 缓存信息
		extractedRequest.cache()
		// 创建Request的数据集
		val requestDataSet = spark.createDataset(extractedRequest)
		//创建Request的临时表
		requestDataSet.createOrReplaceTempView("request")

		// 过滤Response数据
		val filtedRespons = lines.filter(filterResponse)
		// 提取Response数据
		val extractedResponse = filtedRespons.map(extractResponse)
		// 缓存信息
		extractedRequest.cache()
		// 创建Response的数据集
		val responseDataSet = spark.createDataset(extractedResponse)
		// 创建Request的临时表
		responseDataSet.createOrReplaceTempView("response")

		// Join两个表
		val res = spark.sql("select request.token,visitorId,promotionId,reqTimestamp,resultCode,prizeLevel " +
				"from request left join response on request.token=response.token").coalesce(1)
		res.write.option("header", "true")
				.csv(outputPath)

		// 停止整个应用
		spark.stop()
	}

	/**
	  * 根据指定的条件过滤Request数据
	  *
	  * @param infomation 需要判断的信息
	  * @return 是否满足条件
	  */
	def filterRequest(infomation: String): Boolean = {
		// 过滤条件01
		val condition = infomation.matches(".*调用接口 : S010257 - 抽奖 , 请求参数 :.*")
		if (condition) {
			return true
		}
		false
	}

	/**
	  * 从Request提取指定的信息
	  *
	  * @param information 源信息
	  * @return 提取后的信息
	  */
	def extractRequest(information: String): Req = {
		val reqTimestamp = information.split("\\|")(0)
		val token = information.split("\\|")(1)
		val pars = information.split(" 请求参数 : ").last
		val json = JSON.parseObject(pars)
		val visitorId = json.getString("visitorId")
		val promotionId = json.getString("promotionId")
		Req(token, visitorId, promotionId, reqTimestamp)
	}

	/**
	  * 根据指定的条件过滤Response数据
	  *
	  * @param infomation 需要判断的信息
	  * @return 是否满足条件
	  */
	def filterResponse(infomation: String): Boolean = {
		// 过滤条件01
		val condition = infomation.matches(".*\\|\\w{33}_.*\\|" +
				".*返回结果.*apiName.*saleSystem.*tradeWay.*clientType.*clientIp" +
				".*stationId.*apiVersion.*token.*trackToken.*appVersion.*visitorId" +
				".*reqToken.*resultCode.*message.*timestamp.*body.*")
		if (condition) {
			return true
		}
		false
	}

	/**
	  * 从Response提取指定的信息
	  *
	  * @param information 源信息
	  * @return 提取后的信息
	  */
	def extractResponse(information: String): Res = {
		val token = information.split("\\|")(1)
		val response = information.split("返回结果 : ").last
		val json = JSON.parseObject(response)
		val resultCode = json.getString("resultCode")
		val body = json.getJSONObject("body")
		var prizeLevel = ""
		if (null != body) {
			prizeLevel = body.getString("prizeLevel")
		}
		if (null != prizeLevel && prizeLevel.equals("")) {
			prizeLevel = null
		}
		// 封装数据
		Res(token, resultCode, prizeLevel)
	}
}
