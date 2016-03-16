package com.basho.riak.stub

import com.basho.riak.client.core.RiakMessage

/**
  * A tagging interface that all Riak message handlers must extend. The class that is interested in processing an
  * Riak message implements this interface. When the Riak message receives, that object's <code>handle</code> method is
  * invoked.
  */
trait RiakMessageHandler {

  /**
    * Invoked when an Riak message receives.
    */
  def handle(context: ClientHandler.Context, input: RiakMessage): Iterable[RiakMessage]
}
