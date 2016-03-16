package com.basho.riak.spark.rdd.failover

import java.net.InetSocketAddress

import com.basho.riak.spark.rdd.{FullBucketReadTest, RiakTSTests}
import com.basho.riak.stub.{ProxyMessageHandler, RiakNodeStub}
import org.apache.spark.SparkConf
import org.junit.After
import org.junit.experimental.categories.Category
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(value = classOf[Parameterized])
@Category(Array(classOf[RiakTSTests]))
class RiakStubProxyTest(splitSize: Int) extends FullBucketReadTest(splitSize) {

  var riakNodes: Map[InetSocketAddress, RiakNodeStub] = _

  override protected def initSparkConf(): SparkConf = super.initSparkConf()
    .set("spark.riak.connection.host", s"${riakNodes.head._1.getHostString}:${riakNodes.head._1.getPort}")

  override def initialize(): Unit = {
    riakNodes = riakHosts.map { h =>
      val stub = RiakNodeStub(new ProxyMessageHandler(h.getHost, h.getPort))
      stub.start() -> stub
    }.toMap

    super.initialize()
  }

  @After
  override def destroySparkContext(): Unit = {
    Option(riakNodes).foreach(_.foreach { case (_, n) => n.stop() })
    super.destroySparkContext()
  }
}