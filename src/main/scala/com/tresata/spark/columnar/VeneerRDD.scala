package com.tresata.spark.columnar

import org.apache.spark.{ OneToOneDependency, Partition }
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

// RDD that just provides a thin veneer over another RDD which should be persisted, serialized, etc.
// if someone knows a better name i am open to it!
trait VeneerRDD { self: RDD[_] =>
  private def parentRDD: RDD[_] = dependencies.toList match {
    case List(dep: OneToOneDependency[_]) => dep.rdd
    case _ => sys.error("only one-to-one dependency allowed")
  }

  override val partitioner = parentRDD.partitioner

  override protected def getPartitions: Array[Partition] = parentRDD.partitions

  override protected def getPreferredLocations(s: Partition): Seq[String] = parentRDD.preferredLocations(s)

  override def setName(name: String): this.type = { parentRDD.setName(name); this  }

  override def persist(newLevel: StorageLevel): this.type = { parentRDD.persist(newLevel); this }

  override def unpersist(blocking: Boolean = true): this.type = { parentRDD.unpersist(blocking); this }

  override def getStorageLevel: StorageLevel = parentRDD.getStorageLevel
}
