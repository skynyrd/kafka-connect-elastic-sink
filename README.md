In refactoring process, useless readme below. Will be available to clone on 15.09.2017

--------------

## Kafka Connect Elastic Sink Connector

Default Elastic sink connector and open source alternatives read data from Kafka topic, and index/delete them with
respect to bootstrap configuration.

This custom connector created for reading this configuration from data itself.

That is,

* If data has "behaviour" field set to "insert", then connector sends index request.
* If data has "behaviour" field set to "delete", then connector delete request.

"behaviour" flag name is configurable.

### About record
Your record must be a JSON string and these fields should be included:
* flag : Used to get behaviour, should be `insert` or `delete`, field name `flag` is configurable by `flag.field` property. (e.g. `Status` in example configuration below)
* payload: To send Elastic, should contain Json data, field name `payload` is configurable by `data.array.field.name` property. (e.g. `Products` in example configuration below). Payload can be array or object.

#### Example Configuration
```
name=ElasticSinkConnector
topics=product-raw-data
connector.class=com.skynyrd.kafka.ElasticSinkConnector
data.as.array=false
max.retries=3
name=ElasticSinkConnector
topics=products-processed
tasks.max=1
type.name=deneme
elastic.url=elasticsearch
data.array.field.name=Products
index.name=deneme
flag.field=Status
elastic.port=9200
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.storage.StringConverter
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

#### TODOs

* Missing tests.

