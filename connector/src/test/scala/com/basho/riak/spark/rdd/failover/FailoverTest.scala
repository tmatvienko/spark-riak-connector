package com.basho.riak.spark.rdd.failover

import com.basho.riak.client.core.RiakMessage
import com.basho.riak.spark.rdd.{FullBucketReadTest, RiakTSTests}
import com.basho.riak.stub.{ProxyMessageHandler, RiakNodeStub}
import org.junit.{After, Ignore}
import org.junit.experimental.categories.Category
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import shaded.com.basho.riak.protobuf.RiakMessageCodes._

// This test could be un-ignored only after fix https://bashoeng.atlassian.net/browse/SPARK-120
@Ignore("Riak cluster of at least 2 nodes is required for this test.")
@RunWith(value = classOf[Parameterized])
@Category(Array(classOf[RiakTSTests]))
class FailoverTest(splitSize: Int) extends FullBucketReadTest(splitSize) {

  final val STUBS_AMOUNT: Int = 1

  var stubNodes: Seq[RiakNodeStub] = _

  override def initialize(): Unit = {
    assert(riakHosts.size >= STUBS_AMOUNT)

    // start a number of proxies up to STUBS_AMOUNT
    stubNodes = (0 until STUBS_AMOUNT).map { i =>
      val host = riakHosts.toList(i)
      val stub = RiakNodeStub(new ProxyMessageHandler(host) {
        override def onRespond(input: RiakMessage, output: Iterable[RiakMessage]): Unit = input.getCode match {
          case MSG_CoverageReq => stubNodes.head.stop() // stop proxy node after coverage plan sent to client
          case _ => super.onRespond(input, output)
        }
      })
      stub.start()
      stub
    }
    super.initialize()
  }

  @After
  override def destroySparkContext(): Unit = {
    Option(stubNodes).foreach(_.foreach(_.stop()))
    super.destroySparkContext()
  }
}
