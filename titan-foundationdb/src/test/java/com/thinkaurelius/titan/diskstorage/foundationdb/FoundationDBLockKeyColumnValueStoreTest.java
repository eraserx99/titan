package com.thinkaurelius.titan.diskstorage.foundationdb;

import com.thinkaurelius.titan.FoundationDBTestSetup;
import com.thinkaurelius.titan.diskstorage.LockKeyColumnValueStoreTest;

public class FoundationDBLockKeyColumnValueStoreTest extends LockKeyColumnValueStoreTest{
    public FoundationDBStoreManager openStorageManager(int id) {
        return new FoundationDBStoreManager(FoundationDBTestSetup.getFoundationDBGraphConfig());
    }
}
