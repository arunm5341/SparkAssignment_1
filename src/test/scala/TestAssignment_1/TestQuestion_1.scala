package TestAssignment_1

import Assignment_1.Question_1
import org.apache.spark.sql.SparkSession
import org.scalatest._

class TestQuestion_1 extends FunSuite with BeforeAndAfterEach{
  var sparkSession : SparkSession = _
  override def beforeEach() {
    sparkSession = SparkSession.builder().appName("udf testings")
      .master("local")
      .config("", "")
      .getOrCreate()
  }
  assert(Question_1.x(true,true,"src/main/resources/users1.csv")===
    (true,true,"src/main/resources/users1.csv"))

  //assert(Question_1.y(true,true,"src/main/resources/transactions.csv")===(true,true,"src/main/resources/transactions.csv"))

  override def afterEach() {
    sparkSession.stop()
  }

}
