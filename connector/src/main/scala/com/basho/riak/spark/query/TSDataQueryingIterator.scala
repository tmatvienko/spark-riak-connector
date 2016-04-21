package com.basho.riak.spark.query

import com.basho.riak.client.core.query.{ Location, RiakObject }
import org.apache.spark.Logging
import com.basho.riak.spark.util.DataConvertingIterator
import com.basho.riak.spark.util.TSConversionUtil
import org.apache.spark.sql.types.StructType
import com.basho.riak.spark.rdd.TsTimestampBindingType
import com.basho.riak.client.core.query.timeseries.ColumnDescription
import scala.reflect.ClassTag

class TSDataQueryingIterator[R](query: QueryTS)(implicit schema: Option[StructType] = None, tsTimestampBinding: TsTimestampBindingType, ct: ClassTag[R]) extends Iterator[R] with Logging {

  private var _iterator: Option[Iterator[R]] = None
  private val subqueries = query.queryData.iterator
  println(query.queryData.size)

  protected[this] def prefetch() = {
    val r = query.nextChunk(subqueries.next)
    r match {
      case (cds, rows) =>
        logDebug(s"Returned ${rows.size} rows")
        if (this.schema.isDefined && !cds.isEmpty) validateSchema(schema.get, cds)
        _iterator = Some(DataConvertingIterator.createTSConverting((cds, rows), TSConversionUtil.from[R]))
      case _ => _iterator = None
    }
  }

  override def hasNext: Boolean = {
    if (subqueries.hasNext) {
      if (_iterator.isEmpty || (_iterator.isDefined && !_iterator.get.hasNext))
        prefetch()
    }
    _iterator match {
      case Some(it) => it.hasNext
      case None     => false
    }
  }

  override def next(): R = {
    if (!hasNext) {
      throw new NoSuchElementException("next on empty iterator")
    }
    _iterator.get.next
  }

  private def validateSchema(schema: StructType, columns: Seq[ColumnDescription]): Unit = {
    val columnNames = columns.map(_.getName)

    schema.fieldNames.diff(columnNames).toList match {
      case Nil => columnNames.diff(schema.fieldNames) match {
        case Nil =>
        case diff =>
          throw new IllegalArgumentException(s"Provided schema has nothing about the following fields returned by query: ${diff.mkString(", ")}")
      }
      case diff =>
        throw new IllegalArgumentException(s"Provided schema contains fields that are not returned by query: ${diff.mkString(", ")}")
    }
  }
}

object TSDataQueryingIterator {

  def apply[R](query: QueryTS)(implicit schema: Option[StructType] = None, tsTimestampBinding: TsTimestampBindingType, ct: ClassTag[R]): TSDataQueryingIterator[R] = new TSDataQueryingIterator[R](query)
}
