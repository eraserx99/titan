package com.thinkaurelius.titan.diskstorage.foundationdb;

import com.thinkaurelius.titan.FoundationDBTestSetup;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;

public class FoundationDBKeyColumnValueTest extends KeyColumnValueStoreTest {
    public KeyColumnValueStoreManager openStorageManager() {
        return new FoundationDBStoreManager(FoundationDBTestSetup.getFoundationDBConfig());
    }

}
