package com.thinkaurelius.titan.graphdb.foundationdb;

import com.thinkaurelius.titan.graphdb.TitanGraphTest;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

public class FoundationDBGraphTest extends TitanGraphTest {

    public FoundationDBGraphTest() {
        super(getFoundationDBConfig());
    }

    public static Configuration getFoundationDBConfig() {
        BaseConfiguration config = new BaseConfiguration();
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, "foundationdb");
        return config;
    }

}
