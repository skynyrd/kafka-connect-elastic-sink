package com.skynyrd.kafka.service;

import java.io.IOException;
import java.util.Collection;

public interface ElasticService {
    void process(Collection<String> recordsAsString);
    void closeClient() throws IOException;
}
