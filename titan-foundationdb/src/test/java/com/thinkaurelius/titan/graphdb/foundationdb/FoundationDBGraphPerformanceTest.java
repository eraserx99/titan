package com.thinkaurelius.titan.graphdb.foundationdb;

import com.thinkaurelius.titan.FoundationDBTestSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphPerformanceTest;

public class FoundationDBGraphPerformanceTest extends TitanGraphPerformanceTest {
    public FoundationDBGraphPerformanceTest() {
        super(FoundationDBTestSetup.getFoundationDBGraphConfig());
    }
}
