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

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class ZeroMQLogstashRiver extends AbstractRiverComponent implements River {

  private static final String ZEROMQ_LOGSTASH = "zeromq-logstash";
  private static DateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd");

  private Client client;

  private String address = "tcp://127.0.0.1:12345";
  private String dataType = "logs";
  private String prefix = "logstash";
  private int bulkSize = 2000;
  private int flushInterval = 1;

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
      if (zeroMQSettings.containsKey("bulkSize")) {
        String val = (String) zeroMQSettings.get("bulkSize");
        try {
          bulkSize = Integer.parseInt(val);
        } catch (NumberFormatException e) {
          logger.error("option bulksize is not an integer : {}", val);
        }
      }
      if (zeroMQSettings.containsKey("flushInterval")) {
        String val = (String) zeroMQSettings.get("flushInterval");
        try {
          bulkSize = Integer.parseInt(val);
        } catch (NumberFormatException e) {
          logger.error("option flushInterval is not an integer : {}", val);
        }
      }
    }
  }

  @Override
  public void start() {
    logger.info("Starting ZeroMQ Logstash River [{}] using index prefix {}", address, prefix);

    loop = true;
    Runnable consumer = new Consumer(new ZeroMQWrapper(address, logger));
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

  private class Consumer implements Runnable {

    private ZeroMQWrapper zmq;

    private BulkProcessor bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
      @Override
      public void beforeBulk(long executionId, BulkRequest request) {
        logger.debug("Starting bulking {} data", request.numberOfActions());
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
        logger.debug("Successfully bulked {} data", request.numberOfActions());
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
        logger.error("Unable to index data, {}", failure);
      }
    }).setBulkActions(bulkSize).setFlushInterval(TimeValue.timeValueSeconds(flushInterval)).build();

    public Consumer(ZeroMQWrapper zeroMQWrapper) {
      this.zmq = zeroMQWrapper;
    }

    @Override
    public void run() {
      if(zmq.createSocket()) {
        logger.info("Starting ZeroMQ Logstash loop");
        while (loop) {
          if(zmq.poll(500) > 0) {
            IndexRequestBuilder req = client.
                prepareIndex().
                setSource(zmq.receiveMessage()).
                setIndex(computeIndex(prefix)).
                setType(dataType);

            bulkProcessor.add(req.request());
          }
        }
        zmq.closeSocket();
        logger.info("End of ZeroMQ Logstash loop");
      }
    }
  }

}
