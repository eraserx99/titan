package com.thinkaurelius.titan.diskstorage.foundationdb;

import com.thinkaurelius.titan.FoundationDBTestSetup;
import com.thinkaurelius.titan.diskstorage.MultiWriteKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;

public class FoundationDBMultiWriteKeyColumnValueStoreTest extends MultiWriteKeyColumnValueStoreTest {
    public KeyColumnValueStoreManager openStorageManager() {
        return new FoundationDBStoreManager(FoundationDBTestSetup.getFoundationDBConfig());
    }
}
