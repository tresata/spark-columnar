package com.tresata.spark.columnar

import scala.reflect.ClassTag
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.Builder

import shapeless.{ HList, HNil, Poly1, ::, Generic }
import shapeless.ops.hlist.{ Zip, ZipConst, Mapper }

import org.apache.spark.{ OneToOneDependency, Partition, TaskContext }
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import com.twitter.chill.{ KryoInstantiator, ScalaKryoInstantiator }

object Externalizer {
  def apply[T](t: T): Externalizer[T] = {
    val x = new Externalizer[T]
    x.set(t)
    x
  }
}

// chill's Externalizer does not set kryo's classloader which seems necessary for spark so we subclass
class Externalizer[T] extends com.twitter.chill.Externalizer[T] {
  override protected def kryo: KryoInstantiator =
    (new ScalaKryoInstantiator {
      override def newKryo = {
        val kryo = super.newKryo
        kryo.setClassLoader(Thread.currentThread.getContextClassLoader)
        kryo
      }
    }).setReferences(true)
}

// ties type of HA (hlist of arrays) to HE (hlist of elements that go into the arrays)
trait ArrayConstraint[HA <: HList, HE <: HList]

object ArrayConstraint {
  implicit def hnilArrayConstraint = new ArrayConstraint[HNil, HNil] { }

  implicit def hlistArrayConstraint[E, TA <: HList, TE <: HList](implicit tailConstraint: ArrayConstraint[TA, TE]) = 
    new ArrayConstraint[Array[E] :: TA, E :: TE] { }
}

object ColumnarPartition {
  object fIndex extends Poly1 {
    implicit def default[X] = at[(Array[X], Int)]{ case (a, i) => a(i) }
  }
}

class ColumnarPartition[HA <: HList, HE <: HList, HAI <: HList](val data: HA)(
  implicit ac: ArrayConstraint[HA, HE], zc: ZipConst.Aux[Int, HA, HAI], mapper: Mapper[ColumnarPartition.fIndex.type, HAI]
){
  val size = data.toList.headOption.map(_.asInstanceOf[Array[_]].size).getOrElse(0)
  require(data.toList.forall(_.asInstanceOf[Array[_]].size == size))

  def row(i: Int): HE = data.zipConst(i).map(ColumnarPartition.fIndex).asInstanceOf[HE]

  def iterator: Iterator[HE] = Iterator.range(0, size).map{ i => row(i) }
}

// ties types of HE (hlist of elements that go into the arrays) to Out (hlist of array builders)
trait CanBuildArraysFromHList[HE <: HList, Out <: HList] extends Serializable {
  val builders: Out
}

object CanBuildArraysFromHList {
  implicit def canBuildArraysFromHNil = new CanBuildArraysFromHList[HNil, HNil]{ 
    val builders = HNil
  }

  implicit def canBuildArraysFromHList[H, T <: HList, Out <: HList](
    implicit cbf: CanBuildFrom[Nothing, H, Array[H]], canBuildArraysFromTail: CanBuildArraysFromHList[T, Out]
  ) = new CanBuildArraysFromHList[H :: T, Builder[H, Array[H]] :: Out] { 
    val builders = cbf.apply :: canBuildArraysFromTail.builders
  }
}

object ColumnarRDD {
  object fUpdate extends Poly1 {
    implicit def default[X] = at[(Builder[X, Array[X]], X)]{ case (builder, item) => builder += item }
  }

  object fResult extends Poly1 {
    implicit def default[X] = at[Builder[X, Array[X]]]{ _.result }
  }

  def nullOuter(x: AnyRef) {
    val field = x.getClass.getDeclaredFields.find { _.getName == "$outer" }
    field.map(_.setAccessible(true))
    field.map(_.set(x, null))
  }

  def apply[X: ClassTag, HE <: HList : ClassTag, HB <: HList, Out1 <: HList, HA <: HList : ClassTag, HAI <: HList](rdd: RDD[X])(implicit 
    gen: Generic.Aux[X, HE],
    cba: CanBuildArraysFromHList[HE, HB],
    zip: Zip.Aux[HB :: HE :: HNil, Out1],
    mapper1: Mapper[ColumnarRDD.fUpdate.type, Out1],
    mapper2: Mapper.Aux[ColumnarRDD.fResult.type, HB, HA],
    ac: ArrayConstraint[HA, HE],
    zc: ZipConst.Aux[Int, HA, HAI],
    mapper3: Mapper[ColumnarPartition.fIndex.type, HAI]
  ) = {
    nullOuter(gen) // Generic macros generate anonymous inner classes which have pesky unused outer references that make a mess of serialization
    val lockedGen = Externalizer(gen)
    val lockedCba = Externalizer(cba)
    val lockedZip = Externalizer(zip)
    val lockedMapper1 = Externalizer(mapper1)
    val lockedMapper2 = Externalizer(mapper2)
    val lockedAc = Externalizer(ac)
    val lockedZc = Externalizer(zc)
    val lockedMapper3 = Externalizer(mapper3)
    
    val partitionsRDD = rdd.mapPartitions({ it =>
      val builders = lockedCba.get.builders
      it.foreach{ x => builders.zip(lockedGen.get.to(x))(lockedZip.get).map(fUpdate)(lockedMapper1.get) }
      val ha = builders.map(fResult)(lockedMapper2.get)
      Iterator.single(new ColumnarPartition(ha)(lockedAc.get, lockedZc.get, lockedMapper3.get))
    }, false)

    new ColumnarRDD(partitionsRDD, lockedGen)
  }
}

class ColumnarRDD[X: ClassTag, HA <: HList, HE <: HList, HAI <: HList] private (
  val partitionsRDD: RDD[ColumnarPartition[HA, HE, HAI]], lockedGen: Externalizer[Generic.Aux[X, HE]])
    extends RDD[X](partitionsRDD) with VeneerRDD {

  override def compute(split: Partition,context: TaskContext): Iterator[X] =
    firstParent[ColumnarPartition[_, HE, _]].iterator(split, context).next.iterator.map(lockedGen.get.from(_))
}
