package com.basho.riak.spark.rdd.failover

import java.util.concurrent.atomic.AtomicBoolean

import com.basho.riak.client.core.NoNodesAvailableException
import com.basho.riak.spark._
import com.basho.riak.stub.{RiakMessageHandler, RequestBasedMessageHandler, RiakNodeStub}
import org.junit.rules.ExpectedException
import org.junit.{Rule, Test}
import shaded.com.basho.riak.protobuf.RiakKvPB._
import shaded.com.basho.riak.protobuf.RiakPB.RpbPair
import shaded.com.google.protobuf.ByteString

import scala.collection.JavaConverters._

class UnavailableRiakTest extends AbstractFailoverTest {

  val _expectedException: ExpectedException = ExpectedException.none()

  @Rule
  def expectedException: ExpectedException = _expectedException

  override val riakHosts: Int = 2

  var nodeStopped = new AtomicBoolean(false)

  override val riakMessageHandler: Option[RiakMessageHandler] = Some(new RequestBasedMessageHandler {
    override def handleCoverageRequest(req: RpbCoverageReq): RpbCoverageResp = RpbCoverageResp.newBuilder()
      .addAllEntries(riakNodes
        .zip(distributeEvenly(COVERAGE_ENTRIES_COUNT, riakHosts))
        .flatMap {
          case ((a, _), partitionsPerNode) => (0 until partitionsPerNode).map {
            case partitionIndex: Int => RpbCoverageEntry.newBuilder()
              .setIp(ByteString.copyFromUtf8(a.getHostString))
              .setPort(a.getPort)
              // put host and port into coverage context to identify request's server later
              .setCoverContext(ByteString.copyFromUtf8(a.toString)) // scalastyle:ignore
              .setKeyspaceDesc(ByteString.copyFromUtf8(s"StubCoverageEntry-${a.toString}-$partitionIndex"))
              .build()
          }
        }.asJava)
      .build()

    override def handleIndexRequest(req: RpbIndexReq): RpbIndexResp = {
      // stop node which is not currently in use
      riakNodes.find { case (ia, _) => s"${ia.toString}" != req.getCoverContext.toStringUtf8 }
        .foreach {
          case (_, r: RiakNodeStub) if !nodeStopped.getAndSet(true) =>
            r.stop()
            logInfo(s"Node '${r.host}:${r.port}' was stopped to simulate node crash")
          case _ =>
        }


      RpbIndexResp.newBuilder()
        .addKeys(ByteString.copyFromUtf8("k0"))
        .setDone(true)
        .build()
    }

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

  @Test
  def fullBucketReadWithStoppedNodeShouldFail(): Unit = {
    expectedException.expectCause(new RootCauseMatcher(classOf[NoNodesAvailableException]))
    sc.riakBucket[String](NAMESPACE).queryAll().collect()
  }
}
