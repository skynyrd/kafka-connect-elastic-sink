package com.skynyrd.kafka.service;

import com.google.gson.*;
import com.skynyrd.kafka.ElasticSinkConnectorConfig;
import com.skynyrd.kafka.Record;
import com.skynyrd.kafka.client.ElasticClient;
import com.skynyrd.kafka.client.ElasticClientImpl;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class ElasticServiceImpl implements ElasticService {
    private Gson gson;
    private String statusFlag;
    private String indexName;
    private String typeName;
    private ElasticClient elasticClient;
    private String dataListArrayName;
    private static Logger log = LogManager.getLogger(ElasticServiceImpl.class);

    public ElasticServiceImpl(ElasticClient elasticClient, ElasticSinkConnectorConfig config) {
        statusFlag = config.getFlagField();
        indexName = config.getIndexName();
        typeName = config.getTypeName();
        dataListArrayName = config.getDataListArrayField();

        PrepareJsonConverters();

        if(elasticClient == null) {
            try {
                elasticClient = new ElasticClientImpl(config.getElasticUrl(), config.getElasticPort(), config.getElasticClusterName());
            } catch (UnknownHostException e) {
                log.error("The host is unknown, exception stacktrace: " + e.toString());
            }
        }

        this.elasticClient = elasticClient;
    }

    @Override
    public void process(Collection<String> recordsAsString) {
        List<Record> recordList = new ArrayList<>();

        recordsAsString.forEach(record -> {
            try {
                JsonObject recordAsJson = gson.fromJson(record, JsonObject.class);
                String behavior = recordAsJson.get(statusFlag).getAsString();
                JsonArray dataList = recordAsJson.getAsJsonArray(dataListArrayName);

                if(dataList == null) {
                    log.error("Missing data list in record, which is: " + record);
                }

                recordList.add(new Record(dataList, behavior));
            }
            catch (JsonSyntaxException e) {
                log.error("Cannot deserialize json string, which is : " + record);
            }
            catch (Exception e) {
                log.error("Cannot process data, which is : " + record);
            }
        });

        elasticClient.bulkSend(recordList, indexName, typeName);
    }

    @Override
    public void closeClient() throws IOException {
        elasticClient.close();
    }

    private void PrepareJsonConverters() {
        JsonConverter converter = new JsonConverter();
        converter.configure(Collections.singletonMap("schemas.enable", "false"), false);

        gson = new GsonBuilder()
                .setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE)
                .setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
                .create();
    }
}
