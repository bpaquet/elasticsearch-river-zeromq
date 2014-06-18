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

public class ZeroMQLogstashRiver extends AbstractRiverComponent implements River {

  private static final String ZEROMQ_LOGSTASH = "zeromq-logstash";
  private static DateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd");

  private Client client;

  private String address = "tcp://127.0.0.1:12345";

  private String dataType = "logs";

  private String prefix = "logstash";

  private volatile Thread thread;

  private volatile boolean loop;

  @SuppressWarnings("unchecked")
  @Inject
  public ZeroMQLogstashRiver(RiverName riverName, RiverSettings settings, Client client) {
    super(riverName, settings);
    this.client = client;

    if (settings.settings().containsKey(ZEROMQ_LOGSTASH)) {
      Map<String, Object> zeroMQSettings = (Map<String, Object>) settings.settings().get(ZEROMQ_LOGSTASH);
      if (zeroMQSettings.containsKey("address")) {
        address = (String) zeroMQSettings.get("address");
      }
      if (zeroMQSettings.containsKey("dataType")) {
        dataType = (String) zeroMQSettings.get("dataType");
      }
      if (zeroMQSettings.containsKey("prefix")) {
        prefix = (String) zeroMQSettings.get("prefix");
      }
    }
  }

  @Override
  public void start() {
    logger.info("Starting ZeroMQ Logstash River [{}] using index prefix {}", address, prefix);

    loop = true;
    Runnable consumer;
    try {
      Class.forName("zmq.ZMQ");
      consumer = new ConsumerJeromq();
      logger.info("Using ZeroMQ driver JeroMQ {}.{}", zmq.ZMQ.ZMQ_VERSION_MAJOR, zmq.ZMQ.ZMQ_VERSION_MINOR);
    } catch (ClassNotFoundException e) {
      logger.info("Using ZeroMQ driver JZMQ {}.{}", org.zeromq.ZMQ.getMajorVersion(), org.zeromq.ZMQ.getMinorVersion());
      consumer = new ConsumerJZmq();
    }
    thread = EsExecutors.daemonThreadFactory(settings.globalSettings(), ZEROMQ_LOGSTASH).newThread(consumer);
    thread.start();
  }

  @Override
  public void close() {
    logger.info("Closing ZeroMQ Logstash River [{}]", address);
    loop = false;
    try {
      thread.join();
      logger.info("ZeroMQ Logstash River [{}] closed", address);
    } catch (InterruptedException e) {
      logger.error("Interrupted while waiting end of ZeroMQ loop {}", e);
    }
  }

  public static String computeIndex(String prefix) {
    return prefix + "-" + dateFormat.format(new Date());
  }

  private final ActionListener<IndexResponse> listener = new ActionListener<IndexResponse>() {
    @Override
    public void onResponse(IndexResponse arg0) {
    }

    @Override
    public void onFailure(Throwable arg0) {
      logger.error("Unable to index data {}", arg0);
    }
  };

  private class ConsumerJZmq implements Runnable {

    private org.zeromq.ZContext zmq_ctx;

    private org.zeromq.ZMQ.Socket zmq_socket;

    private void createSocket() {
      zmq_ctx = new org.zeromq.ZContext();
      zmq_socket = zmq_ctx.createSocket(org.zeromq.ZMQ.PULL);
      try {
        zmq_socket.bind(address);
        logger.info("ZeroMQ socket bound to {}", address);
      } catch (org.zeromq.ZMQException e) {
        logger.warn("Unable to bind socket to {}: {}", address, e);
        closeSocket();
      }
    }

    private void closeSocket() {
      if (zmq_socket != null) {
        zmq_ctx.destroySocket(zmq_socket);
        logger.info("ZeroMQ socket closed");
        zmq_socket = null;
      }
      if (zmq_ctx != null) {
        zmq_ctx.close();
        logger.info("ZeroMQ context destroyed");
        zmq_ctx = null;
      }
    }

    @Override
    public void run() {
      createSocket();
      if (zmq_socket != null) {
        org.zeromq.ZMQ.PollItem[] poll = new org.zeromq.ZMQ.PollItem[]{new org.zeromq.ZMQ.PollItem(zmq_socket, org.zeromq.ZMQ.Poller.POLLIN)};
        logger.info("Starting ZeroMQ Logstash loop");
        while (loop) {
          if (org.zeromq.ZMQ.poll(poll, 500) > 0) {
            byte[] data = zmq_socket.recv(0);
            IndexRequestBuilder req = client.prepareIndex();
            String index = computeIndex(prefix);
            req.setSource(data);
            req.setIndex(index);
            req.setType(dataType);
            req.execute(listener);
          }
        }
        closeSocket();
        logger.info("End of ZeroMQ Logstash loop");
      }
    }
  }

  private class ConsumerJeromq implements Runnable {

    private zmq.Ctx zmq_ctx;

    private zmq.SocketBase zmq_socket;

    private void createSocket() {
      zmq_ctx = zmq.ZMQ.zmq_init(1);
      zmq_socket = zmq.ZMQ.zmq_socket(zmq_ctx, zmq.ZMQ.ZMQ_PULL);
      boolean ok = zmq.ZMQ.zmq_bind(zmq_socket, address);
      if (ok) {
        logger.info("ZeroMQ socket bound to {}", address);
      } else {
        logger.warn("Unable to bind socket to {}", address);
        closeSocket();
      }
    }

    private void closeSocket() {
      if (zmq_socket != null) {
        zmq.ZMQ.zmq_close(zmq_socket);
        logger.info("ZeroMQ socket closed");
        zmq_socket = null;
      }
      if (zmq_ctx != null) {
        zmq.ZMQ.zmq_term(zmq_ctx);
        logger.info("ZeroMQ context terminated");
        zmq_ctx = null;
      }
    }

    @Override
    public void run() {
      createSocket();
      if (zmq_socket != null) {
        zmq.PollItem[] poll = new zmq.PollItem[]{new zmq.PollItem(zmq_socket, zmq.ZMQ.ZMQ_POLLIN)};
        logger.info("Starting ZeroMQ Logstash loop");
        while (loop) {
          if (zmq.ZMQ.zmq_poll(poll, 500) > 0) {
            zmq.Msg msg = zmq.ZMQ.zmq_recv(zmq_socket, 0);
            IndexRequestBuilder req = client.prepareIndex();
            String index = computeIndex(prefix);
            req.setSource(msg.data());
            req.setIndex(index);
            req.setType(dataType);
            req.execute(listener);
          }
        }
        closeSocket();
        logger.info("End of ZeroMQ Logstash loop");
      }
    }
  }

}
