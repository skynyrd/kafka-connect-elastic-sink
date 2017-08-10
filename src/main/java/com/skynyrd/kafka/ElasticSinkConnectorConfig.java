package com.skynyrd.kafka;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.util.Map;


public class ElasticSinkConnectorConfig extends AbstractConfig {

  public static final String TYPE_NAME = "type.name";
  private static final String TYPE_NAME_DOC = "Elastic type name to use.";
  public static final String ELASTIC_URL = "elastic.url";
  private static final String ELASTIC_URL_DOC = "Elastic URL to connect.";
  public static final String ELASTIC_PORT = "elastic.port";
  private static final String ELASTIC_PORT_DOC = "Elastic PORT to connect.";
  public static final String INDEX_NAME = "index.name";
  private static final String INDEX_NAME_DOC = "Target index name.";
  public static final String FLAG_FIELD_NAME = "flag.field";
  private static final String FLAG_FIELD_NAME_DOC = "Name of the field used as insertion or deletion flag.";
  public static final String DATA_AS_ARRAY = "data.as.array";
  private static final String DATA_AS_ARRAY_DOC = "True if data is array";
  private static final String DATA_LIST_ARRAY_FIELD_NAME = "data.array.field.name";
  private static final String DATA_LIST_ARRAY_FIELD_NAME_DOC = "Name of the field used as data list";
  private static final String MAX_RETRIES = "max.retries";
  private static final String MAX_RETRIES_DOC = "Maximum retry count for requesting Elastic when error occures.";

  public ElasticSinkConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
    super(config, parsedConfig);
  }

  public ElasticSinkConnectorConfig(Map<String, String> parsedConfig) {
    this(conf(), parsedConfig);
  }

  public static ConfigDef conf() {
    return new ConfigDef()
        .define(TYPE_NAME, Type.STRING, Importance.HIGH, TYPE_NAME_DOC)
        .define(ELASTIC_URL, Type.STRING, Importance.HIGH, ELASTIC_URL_DOC)
        .define(INDEX_NAME, Type.STRING, Importance.HIGH, INDEX_NAME_DOC)
        .define(ELASTIC_PORT, Type.INT, Importance.HIGH, ELASTIC_PORT_DOC)
        .define(FLAG_FIELD_NAME, Type.STRING, Importance.HIGH, FLAG_FIELD_NAME_DOC)
        .define(DATA_LIST_ARRAY_FIELD_NAME, Type.STRING, Importance.HIGH, DATA_LIST_ARRAY_FIELD_NAME_DOC)
        .define(MAX_RETRIES, Type.INT, Importance.HIGH, MAX_RETRIES_DOC)
        .define(DATA_AS_ARRAY, Type.BOOLEAN, Importance.HIGH, DATA_AS_ARRAY_DOC);
  }

  public String getTypeName(){
    return this.getString(TYPE_NAME);
  }

  public String getIndexName(){
    return this.getString(INDEX_NAME);
  }

  public String getElasticUrl(){
    return this.getString(ELASTIC_URL);
  }

  public Boolean getDataAsArray(){
    return this.getBoolean(DATA_AS_ARRAY);
  }

  public String getDataListArrayField(){
    return this.getString(DATA_LIST_ARRAY_FIELD_NAME);
  }

  public String getFlagField(){
    return this.getString(FLAG_FIELD_NAME);
  }

  public Integer getElasticPort() {
    return this.getInt(ELASTIC_PORT);
  }

  public Integer getMaxRetries() {
    return this.getInt(MAX_RETRIES);
  }
}
