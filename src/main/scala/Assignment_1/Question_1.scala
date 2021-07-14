package Assignment_1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Question_1 extends App{

    Logger.getLogger("org").setLevel(Level.ERROR)

    def x(header: Boolean,inferSchema: Boolean ,path:String):DataFrame ={
      val spark = SparkSession.builder().master("local").appName("Question1").getOrCreate()
      val df1 = spark.read
        .option("header", header)
        .option("inferSchema", inferSchema)
        .csv(path)
      return df1
    }

    val df1 = x(true,true,"src/main/resources/users.csv")

    def y(header: Boolean,inferSchema: Boolean ,path:String):DataFrame =
    {
      val spark = SparkSession.builder().master("local").appName("Question1").getOrCreate()
      val df2 = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path)
      return df2
    }

    val df2 = y(true,true,"src/main/resources/transactions.csv")

    val stored_data=df1.join(df2,df1("UserID") === df2("UserID"),"inner")

    stored_data.groupBy("Location").count().show()

}
