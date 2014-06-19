package com.bpaquet.elasticsearch.river.zeromq;

import org.elasticsearch.common.logging.ESLogger;

public class ZeroMQWrapper {

  private ESLogger logger;
  private String address;
  private boolean isJeroMQ = false;

  //JeroMQ
  private zmq.Ctx jeroMQCtx;
  private zmq.SocketBase jeroMQSocket;
  private zmq.PollItem[] jeroMQPollItems;

  //zmq
  private org.zeromq.ZContext zmqCtx;
  private org.zeromq.ZMQ.Socket zmqSocket;
  private org.zeromq.ZMQ.PollItem[] zmqPollItems;


  public ZeroMQWrapper(String address, ESLogger logger) {
    this.address = address;
    this.logger = logger;
    try {
      Class.forName("zmq.ZMQ");
      isJeroMQ = true;
      logger.info("Using ZeroMQ driver JeroMQ {}.{}", zmq.ZMQ.ZMQ_VERSION_MAJOR, zmq.ZMQ.ZMQ_VERSION_MINOR);
    } catch (ClassNotFoundException e) {
      logger.info("Using ZeroMQ driver JZMQ {}.{}", org.zeromq.ZMQ.getMajorVersion(), org.zeromq.ZMQ.getMinorVersion());
    }
  }

  public boolean createSocket() {
    if(isJeroMQ) {
      return jeroMqCreateSocket();
    } else {
      return zmqCreateSocket();
    }
  }

  public void closeSocket() {
    if(isJeroMQ) {
      jeroMQCloseSocket();
    } else {
      zmqCloseSocket();
    }
  }

  public int poll(int delay) {
    if(isJeroMQ) {
      return jeroMQPoll(delay);
    } else {
      return zmqPoll(delay);
    }
  }

  public byte[] receiveMessage() {
    if(isJeroMQ) {
      return jeroMQreceiveMessage();
    } else {
      return zmqReceiveMessage();
    }
  }

  /********* JeroMQ *********/

  private boolean jeroMqCreateSocket() {
    jeroMQCtx = zmq.ZMQ.zmq_init(1);
    jeroMQSocket = zmq.ZMQ.zmq_socket(jeroMQCtx, zmq.ZMQ.ZMQ_PULL);
    boolean ok = zmq.ZMQ.zmq_bind(jeroMQSocket, address);
    if (ok) {
      jeroMQPollItems = new zmq.PollItem[] { new zmq.PollItem(jeroMQSocket, zmq.ZMQ.ZMQ_POLLIN) };
      logger.info("ZeroMQ socket bound to {}", address);
    } else {
      logger.warn("Unable to bind socket to {}", address);
      jeroMQCloseSocket();
    }
    return ok;
  }

  private void jeroMQCloseSocket() {
    if (jeroMQSocket != null) {
      zmq.ZMQ.zmq_close(jeroMQSocket);
      logger.info("ZeroMQ socket closed");
      jeroMQSocket = null;
    }
    if (jeroMQCtx != null) {
      zmq.ZMQ.zmq_term(jeroMQCtx);
      logger.info("ZeroMQ context terminated");
      jeroMQCtx = null;
    }
  }

  private int jeroMQPoll(int delay) {
    return zmq.ZMQ.zmq_poll(jeroMQPollItems, delay);
  }

  private byte[] jeroMQreceiveMessage() {
    zmq.Msg msg = zmq.ZMQ.zmq_recv(jeroMQSocket, 0);
    return msg.data();
  }

  /********* ZMQ *********/

  private boolean zmqCreateSocket() {
    zmqCtx = new org.zeromq.ZContext();
    zmqSocket = zmqCtx.createSocket(org.zeromq.ZMQ.PULL);
    try {
      zmqSocket.bind(address);
      zmqPollItems = new org.zeromq.ZMQ.PollItem[] { new org.zeromq.ZMQ.PollItem(zmqSocket, org.zeromq.ZMQ.Poller.POLLIN) };
      logger.info("ZeroMQ socket bound to {}", address);
    } catch (org.zeromq.ZMQException e) {
      logger.warn("Unable to bind socket to {}: {}", address, e);
      zmqCloseSocket();
    }
    return (zmqSocket != null);
  }

  private void zmqCloseSocket() {
    if (zmqSocket != null) {
      zmqCtx.destroySocket(zmqSocket);
      logger.info("ZeroMQ socket closed");
      zmqSocket = null;
    }
    if (zmqCtx != null) {
      zmqCtx.close();
      logger.info("ZeroMQ context destroyed");
      zmqCtx = null;
    }
  }

  private int zmqPoll(int delay) {
    return org.zeromq.ZMQ.poll(zmqPollItems, delay);
  }

  private byte[] zmqReceiveMessage() {
    return zmqSocket.recv(0);
  }


}
