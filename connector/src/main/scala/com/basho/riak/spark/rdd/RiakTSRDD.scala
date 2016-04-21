/**
  * Copyright (c) 2015 Basho Technologies, Inc.
  *
  * This file is provided to you under the Apache License,
  * Version 2.0 (the "License"); you may not use this file
  * except in compliance with the License.  You may obtain
  * a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing,
  * software distributed under the License is distributed on an
  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  * KIND, either express or implied.  See the License for the
  * specific language governing permissions and limitations
  * under the License.
  */
package com.basho.riak.spark.rdd

import com.basho.riak.client.core.query.timeseries.{ Row, ColumnDescription }
import com.basho.riak.spark.query.{ TSQueryData, QueryTS }
import com.basho.riak.spark.rdd.connector.RiakConnector
import com.basho.riak.spark.rdd.partitioner.{ RiakTSPartition, RiakTSPartitioner }
import com.basho.riak.spark.util.{ TSConversionUtil, CountingIterator, DataConvertingIterator }
import org.apache.spark.sql.types.StructType
import org.apache.spark.{ TaskContext, Partition, Logging, SparkContext }
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import org.apache.spark.sql.sources.Filter
import com.basho.riak.spark.rdd.partitioner.{ SinglePartitionRiakTSPartitioner, RangedRiakTSPartitioner }
import com.basho.riak.spark.query.TSDataQueryingIterator

/**
  * @author Sergey Galkin <srggal at gmail dot com>
  * @since 1.1.0
  */
class RiakTSRDD[R] private[spark](
     @transient sc: SparkContext,
     val connector: RiakConnector,
     val bucketName: String,
     val schema:Option[StructType] = None,
     columnNames: Option[Seq[String]] = None,
     whereConstraints: Option[(String, Seq[Any])] = None,
     filters: Array[Filter] = Array(),
     tsRangeFieldName: Option[String] = None,
     val query: Option[String] = None,
     val readConf: ReadConf = ReadConf())
    (implicit val ct: ClassTag[R])
  extends RDD[R](sc, Seq.empty) with Logging {

  override def getPartitions: Array[Partition] = {
    if (filters.isEmpty)
      SinglePartitionRiakTSPartitioner.partitions(connector.hosts, bucketName, schema, columnNames, query, whereConstraints)
    else {
      val partitioner = tsRangeFieldName match {
        case Some(name) => RangedRiakTSPartitioner(connector.hosts, bucketName, schema, columnNames, filters, readConf, name)
        case None       => RangedRiakTSPartitioner(connector, bucketName, schema, columnNames, filters, readConf)
      }
      partitioner.partitions()
    }
  }

  private def computeTS(partitionIdx: Int, context: TaskContext, queryData: Seq[TSQueryData]) = {
    // this implicit values is using to pass parameters to 'com.basho.riak.spark.util.TSConversionUtil$.from'
    implicit val schema = this.schema
    implicit val tsTimestampBinding = readConf.tsTimestampBinding

    val startTime = System.currentTimeMillis()
    val q = new QueryTS(connector, queryData)

    val iterator: Iterator[R] = TSDataQueryingIterator[R](q)
    val countingIterator = CountingIterator[R](iterator)
    context.addTaskCompletionListener { (context) =>
      val endTime = System.currentTimeMillis()
      val duration = (endTime - startTime) / 1000.0
      logDebug(s"Fetched ${countingIterator.count} rows from $bucketName" +
        f" for partition $partitionIdx in $duration%.3f s.")
    }
    countingIterator
  }

  override def compute(split: Partition, context: TaskContext): Iterator[R] = {

    split match {
      case tsp: RiakTSPartition =>
        computeTS(split.index, context, tsp.queryData)

      case _ =>
        throw new IllegalStateException("Unsupported partition type")
    }
  }

  private def copy(
        query: Option[String] = this.query,
        schema: Option[StructType] = schema,
        columnNames: Option[Seq[String]] = columnNames,
        where: Option[(String, Seq[Any])] = whereConstraints,
        readConf: ReadConf = readConf, connector: RiakConnector = connector,
        filters: Array[Filter] = filters,
        tsRangeFieldName: Option[String] = tsRangeFieldName): RiakTSRDD[R] =
    new RiakTSRDD(sc, connector, bucketName, schema, columnNames, where, filters, tsRangeFieldName, query, readConf)

  def select(columns: String*): RiakTSRDD[R] = {
    copy(columnNames = Some(columns))
  }

  def schema(structType: StructType): RiakTSRDD[R] = {
    copy(schema = Option(structType))
  }

  /**
   * Adds a SQL `WHERE` predicate(s) to the query.
   */
  def where(sql: String, values: Any*): RiakTSRDD[R] = {
    copy(where = Some((sql, values)))
  }

  def sql(query: String): RiakTSRDD[R] = {
    copy(query = Some(query))
  }

  def filter(filters: Array[Filter]): RiakTSRDD[R] = {
    copy(filters = filters)
  }

  def partitionByTimeRanges(tsRangeFieldName: String, filters: Array[Filter]): RiakTSRDD[R] = {
    copy(tsRangeFieldName = Some(tsRangeFieldName), filters = filters)
  }
}

/**
  * @author Sergey Galkin <srggal at gmail dot com>
  * @since 1.1.0
  */
object RiakTSRDD {
  def apply[T](sc: SparkContext, bucketName: String, readConf: ReadConf)(implicit ct: ClassTag[T], connector: RiakConnector): RiakTSRDD[T] =
    new RiakTSRDD[T](sc, connector, bucketName, readConf = readConf)

  def apply[T](sc: SparkContext, bucketName: String, readConf: ReadConf, schema: Option[StructType])(implicit ct: ClassTag[T], connector: RiakConnector): RiakTSRDD[T] =
    new RiakTSRDD[T](sc, connector, bucketName, schema, readConf = readConf)
}


