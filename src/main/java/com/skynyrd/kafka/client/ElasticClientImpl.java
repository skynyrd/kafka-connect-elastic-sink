package com.skynyrd.kafka.client;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import com.skynyrd.kafka.Constants;
import com.skynyrd.kafka.Record;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticClientImpl implements ElasticClient {
    private Type dataType = new TypeToken<HashMap<String, Object>>() {}.getType();
    private TransportClient client;
    private Gson gson;
    private static Logger log = LogManager.getLogger(ElasticClientImpl.class);

    public ElasticClientImpl(String url, int port, String clusterName) throws UnknownHostException {
        Settings settings = Settings.builder()
                .put("cluster.name", clusterName).build();

        client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(url), port));

        gson = new GsonBuilder()
                .registerTypeAdapter(Double.class, (JsonSerializer<Double>) (src, typeOfSrc, context) -> {
                    if(src == src.longValue())
                        return new JsonPrimitive(src.longValue());
                    return new JsonPrimitive(src);
                })
                .setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE)
                .setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
                .create();
    }


    @Override
    public void bulkSend(List<Record> records, String index, String type) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();

        records.forEach(r -> {
            String behaviour = r.getBehaviour();
            JsonArray dataList = r.getDataList();

            dataList.forEach(data -> {
                JsonObject dataAsObject = data.getAsJsonObject();
                String id = dataAsObject.get(Constants.dataId).getAsString();

                if(behaviour.equals(Constants.insertedFlagValue)) {
                    Map<String, Object> product = gson.fromJson(data, dataType);
                    bulkRequest.add(client.prepareUpdate(index, type, id).setDoc(product).setDocAsUpsert(true));
                }
                else if(behaviour.equals(Constants.deletedFlagValue)) {
                    bulkRequest.add(client.prepareDelete(index, type, id));
                }
                else
                    log.error("Status flag can not be parsed.");
            });
        });

        bulkRequest.get();
    }

    public void close() throws IOException {
        client.close();
    }
}
