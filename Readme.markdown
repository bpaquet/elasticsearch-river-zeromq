ZeroMQ Logstash River Plugin for ElasticSearch
==================================

[![Build Status](https://travis-ci.org/bpaquet/elasticsearch-river-zeromq.png)](https://travis-ci.org/bpaquet/elasticsearch-river-zeromq)

This plugin allows fast indexing from logstash or [node-logstash](https://github.com/bpaquet/node-logstash), using the ZeroMQ transport.

This plugin use by default [JeroMQ](https://github.com/zeromq/jeromq), a pure Java ZeroMQ implementation. See below to use [JZmq](https://github.com/zeromq/jzmq).

Without this plugin:

* Logstash create an ElasticSearch node and join the cluster. Startup is very slow, and can consume lot of memory.
* Node-logstash use standard HTTP ElasticSearch protocol.

Installation
---

```sh
bin/plugin -install bpaquet/elasticsearch-river-zeromq/0.0.5 --url https://github.com/bpaquet/elasticsearch-river-zeromq/releases/download/elasticsearch-river-zeromq-0.0.5/elasticsearch-river-zeromq-0.0.5.zip
```

How to use it
---

Create a river :

```sh
curl -XPUT localhost:9200/_river/my_river/_meta -d '
{
  "type" : "zeromq-logstash",
  "zeromq-logstash" : {
      "address" : "tcp://127.0.0.1:5556"
  }
}
'
```


From logstash, use

```
output {
  zeromq {
	  topology => "pushpull"
  	mode => "client"
  	address => ["tcp://127.0.0.1:5556"]
  }
}
```

From node-logstash, use

```
output://zeromq://tcp://127.0.0.1:5556
```

That's all !

Use JZMQ
---

* Install the plugin
* Go into plugin directory : ``plugins/river-zeromq``
* Remove jeromq and jzmq jars
* Copy the zmq.jar (standard installation path is ``/usr/share/java/zmq.jar``)
* Set ``java.library.path`` while starting ElasticSearch : ``JAVA_OPTS="-Djava.library.path=/lib" bin/elasticsearch -f``
