ZeroMQ Logstash River Plugin for ElasticSearch
==================================

This plugin allows fast indexing from logstash or [node-logstash](https://github.com/bpaquet/node-logstash), using the ZeroMQ transport instead of HTTP.

Currently, this plugin use [JeroMQ](https://github.com/zeromq/jeromq), a pure Java ZeroMQ implementation.

Installation
---

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