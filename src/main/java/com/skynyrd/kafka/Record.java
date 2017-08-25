package com.skynyrd.kafka;

import com.google.gson.JsonArray;

public class Record {
    private final JsonArray productsAsJsonArray;
    private final String behaviour;

    public Record(JsonArray dataList, String behaviour) {
        this.productsAsJsonArray = dataList;
        this.behaviour = behaviour;
    }

    public String getBehaviour() {
        return behaviour;
    }

    public JsonArray getDataList() {
        return productsAsJsonArray;
    }
}
