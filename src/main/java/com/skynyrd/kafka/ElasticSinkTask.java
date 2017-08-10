package com.skynyrd.kafka;

import com.google.gson.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class ElasticSinkTask extends SinkTask {
    private static Logger log = LoggerFactory.getLogger(ElasticSinkTask.class);
    private ElasticClient elasticClient;
    private Gson gson;
    private String flagField;
    private String indexName;
    private String typeName;
    private Boolean dataAsArray;
    private String dataListArrayField;
    private Integer maxRetries;
    private static final String insert = "insert";
    private static final String delete = "delete";


    @Override
    public String version() {
        return VersionUtil.getVersion();
    }


    @Override
    public void start(Map<String, String> map) {
        ElasticSinkConnectorConfig config = new ElasticSinkConnectorConfig(map);
        flagField = config.getFlagField();
        indexName = config.getIndexName();
        typeName = config.getTypeName();
        dataAsArray = config.getDataAsArray();
        dataListArrayField = config.getDataListArrayField();
        maxRetries = config.getMaxRetries();
        elasticClient = new ElasticClient(config.getElasticUrl(), config.getElasticPort());

        JsonConverter converter = new JsonConverter();
        converter.configure(Collections.singletonMap("schemas.enable", "false"), false);

        gson = new GsonBuilder()
                .setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE)
                .setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
                .create();
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        for (SinkRecord record : collection) {
            String recordAsString = String.valueOf(record.value());

            JsonObject recordAsJson = gson.fromJson(recordAsString, JsonObject.class);
            JsonArray dataList = new JsonArray();

            if(dataAsArray) {
                dataList = recordAsJson.getAsJsonArray(dataListArrayField);
            } else {
                JsonObject data = recordAsJson.getAsJsonObject(dataListArrayField);
                dataList.add(data);
            }

            String status = recordAsJson.get(flagField).getAsString();

            if (Objects.equals(status, insert)) {
                tryProcessData(dataList, true);

            } else if (Objects.equals(status, delete)) {
                tryProcessData(dataList, false);
            }
        }
    }

    private void tryProcessData(JsonArray dataList, boolean isInsert) {
        boolean isSucceed = false;

        for(int retry = 0; retry < maxRetries; retry++) {
            if(isSucceed)
                break;

            try {
                ArrayList<JsonObject> toBeProcessed = new ArrayList<>();

                for(JsonElement data: dataList) {
                    JsonObject dataAsJsonObject = data.getAsJsonObject();
                    dataAsJsonObject.add("isInsert", new JsonPrimitive(isInsert));
                    toBeProcessed.add(dataAsJsonObject);
                }

                elasticClient.bulkProcess(toBeProcessed, indexName, typeName);

                isSucceed = true;

            } catch (IOException e) {
                log.error("Can't send data to Elastic.");
                log.error(e.toString());
            }
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
        log.trace("Flushing the queue");
    }

    @Override
    public void stop() {
        if (elasticClient != null) {
            try {
                elasticClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
