package com.skynyrd.kafka;

import com.google.gson.JsonArray;

public class Record {
    private final JsonArray dataArray;
    private final String behaviour;

    public Record(JsonArray dataList, String behaviour) {
        this.dataArray = dataList;
        this.behaviour = behaviour;
    }

    public String getBehaviour() {
        return behaviour;
    }

    public JsonArray getDataList() {
        return dataArray;
    }
}
