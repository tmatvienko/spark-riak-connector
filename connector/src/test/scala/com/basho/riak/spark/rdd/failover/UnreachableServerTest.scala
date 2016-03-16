package com.basho.riak.spark.rdd.failover

import com.basho.riak.client.core.NoNodesAvailableException
import com.basho.riak.spark._
import com.basho.riak.stub.{RiakMessageHandler, RequestBasedMessageHandler}
import org.apache.spark.SparkConf
import org.junit.rules.ExpectedException
import org.junit.{Rule, Test}
import shaded.com.basho.riak.protobuf.RiakKvPB._
import shaded.com.basho.riak.protobuf.RiakPB.RpbPair
import shaded.com.google.protobuf.ByteString

import scala.collection.JavaConverters._

class UnreachableServerTest extends AbstractFailoverTest {

  val _expectedException: ExpectedException = ExpectedException.none()

  @Rule
  def expectedException: ExpectedException = _expectedException

  override val riakMessageHandler: Option[RiakMessageHandler] = Some(new RequestBasedMessageHandler {
    override def handleCoverageRequest(req: RpbCoverageReq): RpbCoverageResp = {
      val riakHosts = Seq(
        riakNodes.head._1.getHostString -> riakNodes.head._1.getPort,
        "1.1.0.0" -> 9999 // add nonexistent host to coverage plan
      )

      val coveragePlan = RpbCoverageResp.newBuilder()
        .addAllEntries(riakHosts.zip(distributeEvenly(COVERAGE_ENTRIES_COUNT, riakHosts.size))
          .flatMap {
            case ((host, port), partitionsPerNode) => (0 until partitionsPerNode).map {
              case partitionIndex: Int => RpbCoverageEntry.newBuilder()
                .setIp(ByteString.copyFromUtf8(host))
                .setPort(port)
                .setCoverContext(ByteString.copyFromUtf8(""))
                .setKeyspaceDesc(ByteString.copyFromUtf8(s"StubCoverageEntry-$host-$port-$partitionIndex"))
                .build()
            }
          }.asJava)
        .build()

      logInfo(s"Coverage plan prepared for hosts: $riakHosts")
      coveragePlan
    }

    override def handleIndexRequest(req: RpbIndexReq): RpbIndexResp = RpbIndexResp.newBuilder()
      .addKeys(ByteString.copyFromUtf8("k0"))
      .setDone(true)
      .build()


    override def handleGetRequest(req: RpbGetReq): RpbGetResp = RpbGetResp.newBuilder()
      .addContent(RpbContent.newBuilder()
        .setValue(ByteString.copyFromUtf8("v0"))
        .setContentType(ByteString.copyFromUtf8("text/plain"))
        .addAllIndexes(List(RpbPair.newBuilder()
          .setKey(ByteString.copyFromUtf8("i0_int"))
          .setValue(ByteString.copyFromUtf8("indexValue"))
          .build()).asJava)
        .build())
      .build()
  })

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.riak.connections.min", "1")
    .set("spark.riak.connections.max", "1")

  @Test
  def fullBucketReadWithNonexistentShouldFail(): Unit = {
    expectedException.expectCause(new RootCauseMatcher(classOf[NoNodesAvailableException]))
    sc.riakBucket[String](NAMESPACE).queryAll().collect()
  }
}
