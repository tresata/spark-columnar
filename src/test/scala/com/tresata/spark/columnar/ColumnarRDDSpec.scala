package com.tresata.spark.columnar

import org.apache.spark.SparkContext._
import org.scalatest.FunSpec

object ColumnarRDDSpec {
  case class TestClass(value1: Int, value2: Double)
}

class ColumnarRDDSpec extends FunSpec with SparkSuite {
  import ColumnarRDDSpec._

  describe("A ColumnarRdd") {
    it("should act the same as the RDD of tuples its derived from") {
      val rdd = SparkSuite.sc.parallelize(List((1, 1.0), (2, 2.0)))
      val colRdd = ColumnarRDD(rdd)
      assert(colRdd.sameElements(rdd))
    }

    it("should act the same as the RDD of case classes its derived from") {
      val rdd = SparkSuite.sc.parallelize(List(TestClass(1, 1.0), TestClass(2, 2.0)))
      val colRdd = ColumnarRDD(rdd)
      assert(colRdd.sameElements(rdd))
    }
  }
}
