package com.ptb.gaia.partition

import java.util.Random

import org.apache.spark.Partitioner

/**
  * Created by MengChen on 2017/1/19.
  */
class RandomSegmentPartitioner(numParts: Int) extends Partitioner {

  override def numPartitions: Int = numParts

  val random = new Random()

  override def getPartition(key: Any): Int = {

    val id = key.hashCode()
    val code = (id * random.nextInt(10)) % numPartitions
    if (code < 0) {
      code + numPartitions
    } else {
      code
    }
  }

  override def equals(other: Any): Boolean = other match {
    case iteblog: RandomSegmentPartitioner =>
      iteblog.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}