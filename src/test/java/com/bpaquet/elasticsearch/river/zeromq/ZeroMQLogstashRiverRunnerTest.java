package com.bpaquet.elasticsearch.river.zeromq;

import java.io.IOException;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.Before;

import zmq.Ctx;
import zmq.SocketBase;
import zmq.ZMQ;

public abstract class ZeroMQLogstashRiverRunnerTest {

	protected static final String INDEX = ZeroMQLogstashRiver.computeIndex();

	protected Node node;
	
	protected Ctx ctx;
	
	protected SocketBase socket;

	protected abstract XContentBuilder river() throws IOException;
	
	@Before
	public void setup() throws Exception {
		Settings settings = ImmutableSettings.settingsBuilder()
				.put("gateway.type", "none")
				.put("index.number_of_shards", 1)
				.put("index.number_of_replicas", 0)
				.build();
		node = NodeBuilder.nodeBuilder().local(true).settings(settings).node();
		
		try {
            node.client().admin().indices().prepareDelete(INDEX).execute().actionGet();
        } catch (IndexMissingException e) {
        }
        
		node.client().prepareIndex("_river", "my_river", "_meta").setSource(river()).execute().actionGet();

        // wait for river start
        Thread.sleep(200);
	}

	@After
	public void tearDown() {
		if (socket != null) {
			ZMQ.zmq_close(socket);
			socket = null;
		}
		if (ctx != null) {
			ZMQ.zmq_term(ctx);
			ctx = null;
		}
		if (node != null) {
			node.close();
		}
		node = null;
	}
}
