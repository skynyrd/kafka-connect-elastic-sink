## Kafka Connect Elastic Sink Connector

Default Elastic sink connector and open source alternatives read data from Kafka topic, and index/delete them with
respect to bootstrap configuration.

This custom connector created for reading this configuration from data itself.

That is,

* If data has "status" field set to "insert", then connector sends index request.
* If data has "status" field set to "delete", then connector delete request.

"status" flag name is configurable.

### About record
Your record must be a JSON string and these fields should be included:
* flag : Used to get behaviour, should be `insert` or `delete`, field name `flag` is configurable by `flag.field` property. (e.g. `status` in example configuration below)
* You can also change `insert` or `delete` values `Constants.java` file.
* payload: To send Elastic, should contain Json data, field name `dataList` is configurable by `data.array` property. (e.g. `dataList` in example configuration below). Payload can be array or object.
* Your data in `dataList` must include `id` field. You can change id field name from `Constants.java`

#### Example Configuration
```
elastic.url=elasticsearch
name=ElasticSinkConnector
topics=first_topic
tasks.max=1
type.name=targettype
connector.class=com.skynyrd.kafka.ElasticSinkConnector
elastic.port=9200
index.name=targetindex
flag.field=status
data.array=dataList
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=false
key.converter=org.apache.kafka.connect.storage.StringConverter
```


#### Run with local Kafka cluster and Elastic instance

You should have Docker and Docker Compose to run and test it in your local.

* Clone this repo
* `mvn clean` and `mvn install`
* Change absolute path in `volumes` under `kafka-cluster` in `docker-compose.yml`
* Run with `docker-compose up` 
* Wait a bit and open connect console `http://localhost:3030/kafka-connect-ui/#/cluster/fast-data-dev/select-connector`
* Select `ElasticSinkConnector` in right bottom of the page. (Appears if volume is correctly set in compose file.)
* For logs: `http://localhost:3030/logs/connect-distributed.log`

Curious about how I implemented this repository?
[Here is my medium post](https://hackernoon.com/writing-your-own-sink-connector-for-your-kafka-stack-fa7a7bc201ea)

#### TODOs

* Missing tests.

