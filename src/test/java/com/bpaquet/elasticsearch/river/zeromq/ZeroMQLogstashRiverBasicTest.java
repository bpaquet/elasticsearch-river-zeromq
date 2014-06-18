package com.bpaquet.elasticsearch.river.zeromq;

import java.io.IOException;
import java.net.Socket;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Assert;
import org.junit.Test;

import zmq.Msg;
import zmq.ZMQ;

public class ZeroMQLogstashRiverBasicTest extends ZeroMQLogstashRiverRunnerTest {

  @Override
  protected XContentBuilder river() throws IOException {
    return XContentFactory.jsonBuilder().startObject().field("type", "zeromq-logstash").endObject();
  }

  @Test
  public void portOpen() throws Exception {
    // check if ZeroMQ port is open
    Socket s = new Socket("127.0.0.1", 12345);
    s.close();
  }

  @Test
  public void basicIndexTest() throws Exception {
    ctx = ZMQ.zmq_init(1);
    socket = ZMQ.zmq_socket(ctx, ZMQ.ZMQ_PUSH);
    Assert.assertEquals(true, socket.connect("tcp://127.0.0.1:12345"));
    XContentBuilder line = XContentFactory.jsonBuilder().startObject()
        .field("@version", 1)
        .field("@timestamp", "2013-12-31T10:57:13.412Z")
        .field("message", "toto")
        .field("source", "stdin")
        .endObject();
    Msg m = new Msg(line.string().getBytes());
    ZMQ.zmq_send(socket, m, 0);
    // wait for message processing
    Thread.sleep(1000);
    // Force index refresh to avoid long wait
    node.client().admin().indices().prepareRefresh(INDEX).execute().actionGet();

    SearchResponse response = node.client().prepareSearch(INDEX).execute().actionGet();
    Assert.assertEquals(1, response.getHits().getTotalHits());
    Assert.assertEquals("logs", response.getHits().getAt(0).getType());
    Assert.assertEquals("toto", response.getHits().getAt(0).getSource().get("message"));
    Assert.assertEquals("stdin", response.getHits().getAt(0).getSource().get("source"));
    Assert.assertEquals("2013-12-31T10:57:13.412Z", response.getHits().getAt(0).getSource().get("@timestamp"));
  }

}
