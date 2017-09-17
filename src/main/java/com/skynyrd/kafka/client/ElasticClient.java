package com.skynyrd.kafka.client;

import com.skynyrd.kafka.Record;

import java.io.IOException;
import java.util.List;

public interface ElasticClient {
    void bulkSend(List<Record> records, String index, String type) throws IOException;

    void close() throws IOException;
}
