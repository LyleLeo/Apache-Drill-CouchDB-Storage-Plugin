package org.apache.drill.exec.store.couch;

import org.apache.drill.common.logical.StoragePluginConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(CouchStoragePluginConfig.NAME)
public class CouchStoragePluginConfig extends StoragePluginConfig {
    static final Logger logger = LoggerFactory.getLogger(CouchStoragePluginConfig.class);

    public static final String NAME = "couch";
    private String connection;
    private String resultKey; // result key

    @JsonCreator
    public CouchStoragePluginConfig(@JsonProperty("connection") String connection,
                                    @JsonProperty("resultKey") String resultKey) {
        logger.info("initialize CouchStoragePluginConfig {}", connection);
        this.connection = connection;
        this.resultKey = resultKey;
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        } else if (that == null || getClass() != that.getClass()) {
            return false;
        }
        logger.debug("CouchStoragePluginConfig equals {}", connection);
        CouchStoragePluginConfig t = (CouchStoragePluginConfig) that;
        return this.connection.equals(t.connection) && this.resultKey.equals(t.resultKey);
    }

    @Override
    public int hashCode() {
        return this.connection != null ? this.connection.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "CouchStoragePluginConfig[connection=" + connection + "]";
    }

    public String getConnection() {
        return connection;
    }

    public String getResultKey() {
        return resultKey;
    }
}
