package com.skynyrd.kafka;

import com.google.gson.JsonObject;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

class ElasticClient {
    private RestClient restClient;

    ElasticClient(String url, int port){
        restClient = RestClient.builder(
                new HttpHost(url, port, "http")
        ).build();
    }

    void bulkProcess(ArrayList<JsonObject> dataList, String index, String type) throws IOException {
        StringBuilder bulkRequestBody = new StringBuilder();

        for (JsonObject bulkItem : dataList) {

            String behaviour;
            if(bulkItem.get("isInsert").getAsBoolean()) {
                behaviour = "index";
            }
            else {
                behaviour = "delete";
            }

            bulkRequestBody.append(
                    String.format("{ \"%s\" : { \"_index\" : \"%s\", \"_type\" : \"%s\", \"_id\" : \"%s\" } }%n",
                    behaviour,
                    index,
                    type,
                    bulkItem.get("_id").getAsString()));

            bulkRequestBody.append(bulkItem.get("Product").getAsJsonObject());
            bulkRequestBody.append("\n");
        }

        HttpEntity entity = new NStringEntity(bulkRequestBody.toString(), ContentType.APPLICATION_JSON);

        restClient.performRequest("POST",
                String.format("/%s/%s/_bulk", index, type),
                Collections.emptyMap(),
                entity);
    }

    void close() throws IOException {
        restClient.close();
    }
}
