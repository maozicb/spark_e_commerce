package spark.scala

import constant.Constants
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.AccumulatorV2
import utils.StringUtils

class SesssionAggrStatAccumulator extends AccumulatorV2[String, String] {

  var oldStr = Constants.SESSION_COUNT + "=0|" +
    Constants.TIME_PERIOD_1s_3s + "=0|" +
    Constants.TIME_PERIOD_4s_6s + "=0|" +
    Constants.TIME_PERIOD_7s_9s + "=0|" +
    Constants.TIME_PERIOD_10s_30s + "=0|" +
    Constants.TIME_PERIOD_30s_60s + "=0|" +
    Constants.TIME_PERIOD_1m_3m + "=0|" +
    Constants.TIME_PERIOD_3m_10m + "=0|" +
    Constants.TIME_PERIOD_10m_30m + "=0|" +
    Constants.TIME_PERIOD_30m + "=0|" +
    Constants.STEP_PERIOD_1_3 + "=0|" +
    Constants.STEP_PERIOD_4_6 + "=0|" +
    Constants.STEP_PERIOD_7_9 + "=0|" +
    Constants.STEP_PERIOD_10_30 + "=0|" +
    Constants.STEP_PERIOD_30_60 + "=0|" +
    Constants.STEP_PERIOD_60 + "=0"
  var res = ""

  def dealStr(str1: String, str2: String):String = {
    if (StringUtils.isNotEmpty(str1) && StringUtils.isNotEmpty(str2)) {
      var merge=str1
      val splits = (oldStr + "|").split("\\=0\\|")
      for (x <- splits) {
        val sumValue = StringUtils.getFieldFromConcatString(str1, "\\|", x).toInt + StringUtils.getFieldFromConcatString(str2, "\\|", x).toInt
        merge= StringUtils.setFieldInConcatString(merge, "\\|", x, sumValue.toString)
      }
      return  merge
    }
    if(StringUtils.isEmpty(str1)){
     return  str2
    }
    if(StringUtils.isEmpty(str2)){
      return str1
    }
    str1
  }

  override def isZero: Boolean = {
    res == ""
  }

  override def copy(): AccumulatorV2[String, String] = {
    val acc1 = new SesssionAggrStatAccumulator
    acc1.res = this.res
    acc1
  }

  override def reset(): Unit = {
    res = ""
  }

  override def add(v: String): Unit = {
    val oldValue = StringUtils.getFieldFromConcatString(this.oldStr, "\\|", v)
    val newValue = Integer.valueOf(oldValue) + 1
    this.oldStr = StringUtils.setFieldInConcatString(oldStr, "\\|", v, newValue.toString)
    res = oldStr

  }

  override def merge(other: AccumulatorV2[String, String]): Unit = {
    other match {
      case o: SesssionAggrStatAccumulator => {
        res= dealStr(res, o.res)
      }
      case _ => throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
    }
  }

  override def value: String = {
    res
  }
}

object SesssionAggrStatAccumulator {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName(Constants.SPARK_APP_NAME)
      .config("spark.driver.host", "192.168.149.1")
      .enableHiveSupport().master("local[*]")
      .getOrCreate()
    val acc = new SesssionAggrStatAccumulator
    sparkSession.sparkContext.register(acc, "myacc")
    val rdd = sparkSession.sparkContext.parallelize(Array(Constants.STEP_PERIOD_1_3, Constants.STEP_PERIOD_4_6,Constants.TIME_PERIOD_1s_3s))
    rdd.foreach(
      row => {
        acc.add(row)
      })
    println(acc.value)

  }
}



