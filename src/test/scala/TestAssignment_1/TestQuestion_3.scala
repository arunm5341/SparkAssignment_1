package TestAssignment_1
import Assignment_1.Question_3
import org.apache.spark.sql.SparkSession
import org.scalatest._

class TestQuestion_3 extends FunSuite with BeforeAndAfterEach {

    var sparkSession : SparkSession = _
    override def beforeEach() {
      sparkSession = SparkSession.builder().appName("udf testings")
        .master("local")
        .config("", "")
        .getOrCreate()
    }
    assert(Question_3.x(true,true,"src/main/resources/users.csv")===
      (true,true,"src/main/resources/users1.csv"))

    override def afterEach() {
      sparkSession.stop()
    }
  }




