### KafkaStreamsTest

* flow
  * collectd(kafka plugin) -> kafka -> kafka streams(min/max/avg/sum) -> other topic
  
* kafka stream version
  * kafka-streams -> 1.0.0
  * kafka-clients -> 1.0.0
  
* test type
  * Processor Api
    * ProcessorApiTestMain.java
  * DSL 
    * DslTestMain.java
    
* role
  * collectd collect interval : 10 sec
  * aggregation size(window size) : 60 sec
  
