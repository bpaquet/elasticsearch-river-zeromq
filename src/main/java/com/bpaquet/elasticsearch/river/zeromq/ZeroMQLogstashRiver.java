/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.bpaquet.elasticsearch.river.zeromq;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.script.ScriptService;

import zmq.Ctx;
import zmq.Msg;
import zmq.SocketBase;
import zmq.ZMQ;

/**
 *
 */
public class ZeroMQLogstashRiver extends AbstractRiverComponent implements River {

	private static DateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd");
	
    private Client client;
	
	private String address = "tcp://127.0.0.1:12345";
	
	private String dataType = "logs";

    private volatile Thread thread;

	private Ctx zmq_ctx;

	private SocketBase zmq_socket;
	
	@SuppressWarnings("unchecked")
	@Inject
    public ZeroMQLogstashRiver(RiverName riverName, RiverSettings settings, Client client, ScriptService scriptService) {
        super(riverName, settings);
        this.client = client;
        
        if (settings.settings().containsKey("zeromq-logstash")) {
            Map<String, Object> zeroMQSettings = (Map<String, Object>) settings.settings().get("zeromq-logstash");
            if (zeroMQSettings.containsKey("address")) {
            	address = (String) zeroMQSettings.get("address");
            }
            if (zeroMQSettings.containsKey("dataType")) {
            	dataType = (String) zeroMQSettings.get("dataType");
            }
        }
   }
 
	@Override
	public void start() {
	    logger.info("Starting ZeroMQ Logstash River [{}]", address);

	    thread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "zeromq-logstash").newThread(new Consumer());
	    thread.start();
	}

    @Override
    public void close() {
    	if (thread.isAlive()) {
    		logger.info("Closing ZeroMQ Logstash River [{}]", address);
    		thread.interrupt();
    	}
    }
    
    private void createSocket() {
    	zmq_ctx = ZMQ.zmq_init(1);
        zmq_socket = ZMQ.zmq_socket(zmq_ctx, ZMQ.ZMQ_PULL);
        boolean ok = ZMQ.zmq_bind(zmq_socket, address);
        if (ok) {
        	logger.info("ZeroMQ socket closed to {}", address);
        }
        else {
        	logger.warn("Unable to bind socket to {}", address);
        	closeSocket();
        }
    }
    
    private void closeSocket() {
    	if (zmq_socket != null) {
    		ZMQ.zmq_close(zmq_socket);
        	logger.info("ZeroMQ socket closed");
        	zmq_socket = null;
    	}
    	if (zmq_ctx != null) {
    		ZMQ.zmq_term(zmq_ctx);
    		zmq_ctx = null;
    	}
    }
    
    private String computeIndex() {
    	return "logstash-" + dateFormat.format(new Date());
    }
    
    private class Consumer implements Runnable {

        @Override
        public void run() {
        	createSocket();
        	if (zmq_socket != null) {
        		logger.info("Starting ZeroMQ Logstash loop");
        		while (true) {
            		Msg msg = ZMQ.zmq_recv(zmq_socket, 0);
            		IndexRequestBuilder req = client.prepareIndex();
            		req.setSource(msg.data());
            		req.setIndex(computeIndex());
            		req.setType(dataType);
            		req.execute(new ActionListener<IndexResponse>() {
						
						@Override
						public void onResponse(IndexResponse arg0) {
						}
						
						@Override
						public void onFailure(Throwable arg0) {
							logger.warn("Unable to index data {}", arg0);
						}
					});
            		msg.close();
            	}
    		}
        }
    }

}
