package spark.scala

import java.{lang, util}

import com.alibaba.fastjson.{JSON, JSONObject}
import constant.Constants
import dao.DAOFactory
import dao.domain.{SessionAggrStat, SessionDetail, Top10Category, Top10Session}
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import utils.{NumberUtils, ParamUtils, StringUtils}

object UserVisitSessionAnalyzeSpark {


  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName(Constants.SPARK_APP_NAME)
      //.config("spark.driver.host", "192.168.149.1")
      // .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
     //  .master("local[*]")
      //  .master("spark://192.168.149.100:7077")
      .getOrCreate()
    //创建需要使用的DAO组件
    val taskDAO = DAOFactory.getTaskDAO

    //那么就首先得查询出来指定的任务，并获取任务的查询参数 取args[0]
    val taskid = ParamUtils.getTaskIdFromArgs(args)
    val task = taskDAO.findById(taskid)
    val taskParam = JSON.parseObject(task.getTaskParam)

    val fullAggrInfo = aggregateBySession(sparkSession, taskParam)
    fullAggrInfo.persist(StorageLevel.MEMORY_AND_DISK)

    //注册自定义累加器
    val sesssionAggrStatAccumulator = new SesssionAggrStatAccumulator()
    sparkSession.sparkContext.register(sesssionAggrStatAccumulator, "myaccu")

    // 根据Task 过滤出需要的数据
    val filtereFullAggrInfo = fullAggrInfo.filter(filterSession(_, taskParam, sesssionAggrStatAccumulator))
    filtereFullAggrInfo.count()
    //获取热门10个类品
    val topTenCategroy = getTopTenCategoryID(sparkSession, fullAggrInfo, taskid)

    //获取每个品类top10 点击链接
    getTopTenClickSession(sparkSession, fullAggrInfo, taskid)

    //获取每个小时段的session数量
    //  val perHourSeeionCount = randomExtractSession(fullAggrInfo)

    //分析完的数据写入数据库
    calculateAndPersistAggrStat(sesssionAggrStatAccumulator.value, task.getTaskid)

  }

  /**
    * 获取指定日期范围内的用户访问行为数据
    *
    * @param sqlContext SQLContext
    * @param taskParam  任务参数
    * @return 行为数据RDD
    */
  private def aggregateBySession(sparkSession: SparkSession, taskParam: JSONObject): Dataset[Row] = {
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)
    val visitActionSql = "select session_id " +
      ",user_id " +
      ",concat_ws('\\001',collect_list(search_keyword)) as search_keywords" +
      ",concat_ws('\\001',collect_list(user_visit_action.click_category_id)) as clickCategoryIds" +
      ",concat_ws('\\001',collect_list(user_visit_action.order_category_ids)) as order_category_ids" +
      ",concat_ws('\\001',collect_list(user_visit_action.pay_category_ids)) as pay_category_ids" +
      ", unix_timestamp(max(action_time))-unix_timestamp(min(action_time)) as visitLength  " +
      ", count(session_id) as stepLength " +
      ", min(action_time) as startTime" +
      " from user_visit_action " +
      " where datestr>='" + startDate + "'" + " and datestr<='" + endDate + "'" +
      " group by session_id ,user_id "

    val visitActionInfo = sparkSession.sql(visitActionSql)

    val userSql = "select userid,age,professional,city,sex  from default.user_info "
    val userInfo = sparkSession.sql(userSql)
    //fullAggrInfo=   partAggrInfo+    age,professional,city,sex
    // partAggrInfo=sessid,search_keywords(聚合),clickCategoryIds(combine),visitLength(endTime.getTime() - startTime.getTime()) ,stepLength(sessid 个数),startTime
    val fullAggrInfo = visitActionInfo.join(userInfo, visitActionInfo("user_id") === userInfo("userid"))
      .select("session_id", "search_keywords", "clickCategoryIds", "visitLength", "stepLength", "startTime",
        "age", "professional", "city", "sex", "order_category_ids", "pay_category_ids")
    fullAggrInfo
  }

  def filterSession(row: Row, taskParam: JSONObject, sesssionAggrStatAccumulator: SesssionAggrStatAccumulator): Boolean = {
    import constant.Constants
    import utils.ParamUtils
    val startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    val professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

    if (StringUtils.isNotEmpty(startAge) || StringUtils.isNotEmpty(endAge)) {
      if ((row.getAs[Int]("age") < startAge.toInt) || (row.getAs[Int]("age") > endAge.toInt))
        return false
    }
    if (StringUtils.isNotEmpty(professionals)) {
      if (!professionals.contains(row.getAs[String]("professional"))) return false
    }

    if (StringUtils.isNotEmpty(cities)) {
      if (!cities.contains(row.getAs[String]("city"))) return false
    }

    if (StringUtils.isNotEmpty(sex)) {
      if (!sex.equals(row.getAs[String]("sex"))) return false
    }
    var flag = true;
    if (StringUtils.isNotEmpty(keywords)) {
      for (key <- keywords.split(",") if flag) {
        if (row.getAs[String]("search_keywords").contains(key)) flag = false
        false
      }
    }

    flag = true;
    if (StringUtils.isNotEmpty(categoryIds)) {
      for (key <- categoryIds.split(",") if flag) {
        if (row.getAs[String]("clickCategoryIds").contains(key)) flag = false
        false
      }
    }
    //计数器累加session_count，visitlength，setplength
    sesssionAggrStatAccumulator.add(Constants.SESSION_COUNT)
    calculateVisitLength(row.getAs[Long]("visitLength"), sesssionAggrStatAccumulator)
    calculateStepLength(row.getAs[Long]("stepLength"), sesssionAggrStatAccumulator)
    true
  }

  /**
    * 计数器记录访问时长
    *
    * @param visitLength
    * //    */
  def calculateVisitLength(visitLength: Long, sessionAggrAccumulator: SesssionAggrStatAccumulator): Unit = {
    if (visitLength >= 1 && visitLength <= 3) sessionAggrAccumulator.add(Constants.TIME_PERIOD_1s_3s)
    else if (visitLength >= 4 && visitLength <= 6) sessionAggrAccumulator.add(Constants.TIME_PERIOD_4s_6s)
    else if (visitLength >= 7 && visitLength <= 9) sessionAggrAccumulator.add(Constants.TIME_PERIOD_7s_9s)
    else if (visitLength >= 10 && visitLength <= 30) sessionAggrAccumulator.add(Constants.TIME_PERIOD_10s_30s)
    else if (visitLength > 30 && visitLength <= 60) sessionAggrAccumulator.add(Constants.TIME_PERIOD_30s_60s)
    else if (visitLength > 60 && visitLength <= 180) sessionAggrAccumulator.add(Constants.TIME_PERIOD_1m_3m)
    else if (visitLength > 180 && visitLength <= 600) sessionAggrAccumulator.add(Constants.TIME_PERIOD_3m_10m)
    else if (visitLength > 600 && visitLength <= 1800) sessionAggrAccumulator.add(Constants.TIME_PERIOD_10m_30m)
    else if (visitLength > 1800) sessionAggrAccumulator.add(Constants.TIME_PERIOD_30m)

  }

  /**
    * 计数器累计步长
    *
    * @param stepLength
    * @param sessionAggrAccumulator
    */
  def calculateStepLength(stepLength: Long, sessionAggrAccumulator: SesssionAggrStatAccumulator): Unit = {
    if (stepLength >= 1 && stepLength <= 3) sessionAggrAccumulator.add(Constants.STEP_PERIOD_1_3)
    else if (stepLength >= 4 && stepLength <= 6) sessionAggrAccumulator.add(Constants.STEP_PERIOD_4_6)
    else if (stepLength >= 7 && stepLength <= 9) sessionAggrAccumulator.add(Constants.STEP_PERIOD_7_9)
    else if (stepLength >= 10 && stepLength <= 30) sessionAggrAccumulator.add(Constants.STEP_PERIOD_10_30)
    else if (stepLength > 30 && stepLength <= 60) sessionAggrAccumulator.add(Constants.STEP_PERIOD_30_60)
    else if (stepLength > 60) sessionAggrAccumulator.add(Constants.STEP_PERIOD_60)
  }

  /**
    * 将计数器的内容写入到关系型数据库中（mysql）
    *
    * @param value
    * @param taskid
    */
  private def calculateAndPersistAggrStat(value: String, taskid: Long): Unit = {
    //从Accumulator统计串中获取值
    if (value.isEmpty) {
      throw new RuntimeException("计数器中没有数据")
    }
    val session_count = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.SESSION_COUNT)).toLong
    val visit_length_1s_3s = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1s_3s)).toLong
    val visit_length_4s_6s = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_4s_6s)).toLong
    val visit_length_7s_9s = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_7s_9s)).toLong
    val visit_length_10s_30s = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10s_30s)).toLong
    val visit_length_30s_60s = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30s_60s)).toLong
    val visit_length_1m_3m = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1m_3m)).toLong
    val visit_length_3m_10m = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_3m_10m)).toLong
    val visit_length_10m_30m = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10m_30m)).toLong
    val visit_length_30m = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30m)).toLong
    val step_length_1_3 = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_1_3)).toLong
    val step_length_4_6 = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_4_6)).toLong
    val step_length_7_9 = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_7_9)).toLong
    val step_length_10_30 = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_10_30)).toLong
    val step_length_30_60 = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_30_60)).toLong
    val step_length_60 = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_60)).toLong

    //计算各个访问时长和访问步长的范围
    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s.toDouble / session_count.toDouble, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s.toDouble / session_count.toDouble, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s.toDouble / session_count.toDouble, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s.toDouble / session_count.toDouble, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s.toDouble / session_count.toDouble, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m.toDouble / session_count.toDouble, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m.toDouble / session_count.toDouble, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m.toDouble / session_count.toDouble, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m.toDouble / session_count.toDouble, 2)
    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3.toDouble / session_count.toDouble, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6.toDouble / session_count.toDouble, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9.toDouble / session_count.toDouble, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30.toDouble / session_count.toDouble, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60.toDouble / session_count.toDouble, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60.toDouble / session_count.toDouble, 2)
    //将访问结果封装成Domain对象
    val sessionAggrStat = new SessionAggrStat
    sessionAggrStat.setTaskid(taskid)
    sessionAggrStat.setSession_count(session_count)
    sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio)
    sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio)
    sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio)
    sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio)
    sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio)
    sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio)
    sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio)
    sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio)
    sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio)
    sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio)
    sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio)
    sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio)
    sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio)
    sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio)
    sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio)
    //调用对用的DAO插入统计结果
    val sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO
    sessionAggrStatDAO.insert(sessionAggrStat)
  }

  /**
    * 获取top10 类别商品
    *
    */
  def getTopTenCategoryID(sparkSession: SparkSession, df: DataFrame, taskid: Long) = {
    //      .select("session_id", "search_keywords", "clickCategoryIds", "visitLength", "stepLength", "startTime",
    //        "age", "professional", "city", "sex","order_category_ids","pay_category_ids")
    import org.apache.spark.sql.functions._
    import sparkSession.implicits._
    //    val topTenDF = dataFrame.select("clickCategoryIds", "order_category_ids", "pay_category_ids")
    //    val topTenCategoryID = topTenDF.map(row => row.getString(0) + "\001" + row.getString(1) + "\001" + row.getString(2))
    //      .flatMap(_.replaceAll("null", "").split("\\001"))
    //     .map((_, 1)).rdd.reduceByKey((_ + _)).sortBy(_._2, false, 1)
    //获取click,order ,pay id的次数和
    val topTenClick = df.select("clickCategoryIds").map(row => row.getString(0))
      .flatMap(_.split("\\001")).map((_, 1)).toDF("clickid", "clickcount")
      .groupBy("clickid").agg(sum("clickcount")).toDF("clickId", "sumClickCount")

    val topTenOrder = df.select("order_category_ids").map(row => row.getString(0))
      .flatMap(_.split("\\001")).map((_, 1)).toDF("orderid", "ordercount")
      .groupBy("orderid").agg(sum("ordercount")).toDF("orderId", "sumOrderCount")

    val topTenPay = df.select("pay_category_ids").map(row => row.getString(0))
      .flatMap(_.split("\\001")).map((_, 1)).toDF("payid", "paycount")
      .groupBy("payid").agg(sum("paycount")).toDF("payId", "sumPayCount")

    val topTenCategroy = topTenClick.join(topTenOrder, topTenClick("clickId") === topTenOrder("orderId")).join(topTenPay, topTenClick("clickId") === topTenPay("payId"))
      .select("clickId", "sumClickCount", "sumOrderCount", "sumPayCount").where("clickId!='null'")
      .sort($"sumClickCount".desc, $"sumOrderCount".desc, $"sumPayCount".desc).limit(10)

    val top10CategoryDAO = DAOFactory.getTop10CategoryDAO
    val category = new Top10Category
    topTenCategroy.foreach({ row =>
      category.setTaskid(taskid)
      category.setCategoryid(row.getString(0).toLong)
      category.setClickCount(row.getLong(1))
      category.setOrderCount(row.getLong(2))
      category.setPayCount(row.getLong(3))
      top10CategoryDAO.insert(category)
    })

    topTenCategroy
  }

  //  /**
  //    *
  //    */
  //  def randomExtractSession(fullAggrInfo: DataFrame, sparkSession: SparkSession): DataFrame = {
  //    //  import  sparkSession.implicits._
  //    //  val fullAggrInfo = visitActionInfo.join(userInfo, visitActionInfo("user_id") === userInfo("userid"))
  //    //    .select("session_id", "search_keywords", "clickCategoryIds", "visitLength", "stepLength", "startTime",
  //    //      "age", "professional", "city", "sex")
  //    val encoder = Encoders.tuple(Encoders.STRING, Encoders.STRING)
  //    //得到每天每小时的session数量
  //    val countMap = fullAggrInfo.map(row => (row.getString(5), DateUtils.getDateHour(row.getString(0))))(encoder).rdd.countByKey()
  //
  //
  //    /**
  //      * 使用按比例随机抽取算法，计算每个小时抽取的session索引
  //      */
  //
  //    //将<yyyy-mm-dd_hh,count>格式的map转换成<yyyy-mm-dd,<hh,count>>的格式，方便后面使用
  //
  //    val dateHourCountMap = new mutable.HashMap[String, mutable.HashMap[String, Long]]
  //
  //    for ((k, v) <- countMap) {
  //      val date = k.split(_)(0)
  //      val hour = k.split(_)(1)
  //      val count = v
  //
  //      var hourCountMap = dateHourCountMap.getOrElse(date.toString(), null)
  //      if (hourCountMap == null) {
  //        hourCountMap = new mutable.HashMap[String, Long]
  //        dateHourCountMap += (date.toString() -> hourCountMap)
  //      }
  //      hourCountMap += (hour.toString() -> count)
  //    }
  //
  //    var dateHourExtractMap = new mutable.HashMap[String, mutable.HashMap[String, Array[Int]]]()
  //    val random = new Random()
  //    for ((k, v) <- dateHourCountMap) {
  //      val date = k
  //      val hourCountMap = v
  //
  //      //计算这一天session总数
  //      var sessionCount = 0L
  //      for ((k, v) <- hourCountMap) {
  //        sessionCount += v
  //      }
  //   var   hourExtractMap=dateHourExtractMap.getOrElse(date,null)
  //      if(hourExtractMap==null){
  //        hourExtractMap=new mutable.HashMap[String,Array[Int]]()
  //        dateHourExtractMap+=(date->hourExtractMap)
  //      }
  //
  //      for((k,v)<-hourCountMap){
  //        val hour=k
  //        val count=v
  //        //计算每个小时的session数量，占据当天session数量的比例，乘以当天要抽取的数量，可以计算出
  //        //每个小时所需抽取的数量
  //       val  hourExtractNumber= ( count.toDouble/sessionCount.toDouble * extractNumberPerDay).toInt
  //
  //      }
  //    }
  //
  // }

  def getTopTenClickSession(sparkSession: SparkSession, fullAggrInfo: Dataset[Row], taskid: lang.Long) = {
    //      .select("session_id", "search_keywords", "clickCategoryIds", "visitLength", "stepLength", "startTime",
    //        "age", "professional", "city", "sex","order_category_ids","pay_category_ids")
    import org.apache.spark.sql.functions._
    import sparkSession.implicits._
    val dealFullAggrInfo = fullAggrInfo.select("session_id", "clickCategoryIds").withColumn("ids", split(col("clickCategoryIds"), "\001"))
      .withColumn("ids2", explode(col("ids"))).select("session_id", "ids2").where("ids2!='null'")
      .groupBy("session_id", "ids2").agg(count("ids2")).toDF("session_id", "ids2", "count")
    dealFullAggrInfo.createOrReplaceGlobalTempView("dealFullAggrInfo")
    val sql = "select session_id,ids2,count ,row_number() over(partition by ids2 order by count desc ) from global_temp.dealFullAggrInfo "

    val topTenClick = sparkSession.sql(sql).toDF("session_id", "ids2", "count", "rank").where("rank <=10 ")
    topTenClick.persist(StorageLevel.MEMORY_AND_DISK)
    //topTenClick.count()
   // topTenClick.createOrReplaceGlobalTempView("topTenClick")
    val topTenClickBroadCast = sparkSession.sparkContext.broadcast(topTenClick)
    val top10SessionDAO = DAOFactory.getTop10SessionDAO

    val topTenClickBroadCastArray = topTenClickBroadCast.value.map(row => row.getString(0)).collect().toSet

    val top10Session = new Top10Session
    topTenClick.foreach(
      row => {
        top10Session.setTaskid(taskid)
        top10Session.setSessionid(row.getString(0))
        top10Session.setCategoryid(row.getString(1).toLong)
        top10Session.setClickCount(row.getLong(2))
        top10SessionDAO.insert(top10Session)
      }
    )

    val sessionDetailDAO = DAOFactory.getSessionDetailDAO

    //通过广播小变量的形式来避免join 产生的shuffle
    val topTenClickSession = sparkSession.table("user_visit_action").filter(
      row =>
        topTenClickBroadCastArray.contains(row.getString(2))
    )
    //使用foreachPartion 代替fordach 提高并行度
    topTenClickSession.foreachPartition(foreachPartition => {
//      val arr= new util.LinkedList[SessionDetail]()
       val arr= new util.HashSet[SessionDetail]()
      foreachPartition.foreach(
        row => {
          val sessionDetail = new SessionDetail
        sessionDetail.setTaskid(taskid)
        sessionDetail.setUserid(row.getString(1).toLong)
        sessionDetail.setSessionid(row.getString(2))
        sessionDetail.setPageid(row.getString(3).toLong)
        sessionDetail.setActionTime(row.getString(4))
        //          sessionDetail.setSearchKeyword(row.getString(5))
        sessionDetail.setSearchKeyword("test")
        sessionDetail.setClickCategoryId(row.getString(6))
        sessionDetail.setClickProductId(row.getString(7))
        sessionDetail.setOrderCategoryIds((row.getString(8)))
        sessionDetail.setOrderProductIds((row.getString(9)))
        sessionDetail.setPayCategoryIds((row.getString(10)))
        sessionDetail.setPayProductIds((row.getString(11)))
          arr.add(sessionDetail)
      })
      sessionDetailDAO.insertBatch(arr)
    })
    //    topTenClickSession.foreach(
    //            row => {
    //              sessionDetail.setTaskid(taskid)
    //              sessionDetail.setUserid(row.getString(1).toLong)
    //              sessionDetail.setSessionid(row.getString(2))
    //              sessionDetail.setPageid(row.getString(3).toLong)
    //              sessionDetail.setActionTime(row.getString(4))
    //              //          sessionDetail.setSearchKeyword(row.getString(5))
    //              sessionDetail.setSearchKeyword("test")
    //              sessionDetail.setClickCategoryId(row.getString(6))
    //              sessionDetail.setClickProductId(row.getString(7))
    //              sessionDetail.setOrderCategoryIds((row.getString(8)))
    //              sessionDetail.setOrderProductIds((row.getString(9)))
    //              sessionDetail.setPayCategoryIds((row.getString(10)))
    //              sessionDetail.setPayProductIds((row.getString(11)))
    //              sessionDetailDAO.insert(sessionDetail)
    //            })

    //通过spark sql join 方式，但会有shuffle
    //    sparkSession.sql("select b.* from global_temp.topTenClick a , user_visit_action b where a.session_id=b.session_id")
    //      .foreach(
    //        row => {
    //          sessionDetail.setTaskid(taskid)
    //          sessionDetail.setUserid(row.getString(1).toLong)
    //          sessionDetail.setSessionid(row.getString(2))
    //          sessionDetail.setPageid(row.getString(3).toLong)
    //          sessionDetail.setActionTime(row.getString(4))
    //          //          sessionDetail.setSearchKeyword(row.getString(5))
    //          sessionDetail.setSearchKeyword("test")
    //          sessionDetail.setClickCategoryId(row.getString(6))
    //          sessionDetail.setClickProductId(row.getString(7))
    //          sessionDetail.setOrderCategoryIds((row.getString(8)))
    //          sessionDetail.setOrderProductIds((row.getString(9)))
    //          sessionDetail.setPayCategoryIds((row.getString(10)))
    //          sessionDetail.setPayProductIds((row.getString(11)))
    //          sessionDetailDAO.insert(sessionDetail)
    //        })
  }
}
