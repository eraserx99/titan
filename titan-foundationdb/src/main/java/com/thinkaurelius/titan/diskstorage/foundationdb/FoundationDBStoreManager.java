package com.thinkaurelius.titan.diskstorage.foundationdb;

import com.foundationdb.Database;
import com.foundationdb.FDB;
import com.foundationdb.FDBError;
import com.foundationdb.Transaction;
import com.foundationdb.tuple.Tuple;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import org.apache.commons.configuration.Configuration;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FoundationDBStoreManager implements KeyColumnValueStoreManager {

    private final Database db;
    private final ConcurrentHashMap<String, FoundationDBKeyColumnValueStore> openStores;
    private final StoreFeatures features;

    public final String dbname;

    public FoundationDBStoreManager(Configuration config) {
        dbname = config.getString("tablename", "titan");

        FDB fdb = FDB.selectAPIVersion(21);
        db = fdb.open().get();
        openStores = new ConcurrentHashMap<String, FoundationDBKeyColumnValueStore>();
        features = new StoreFeatures();

        features.supportsScan=true;
        features.supportsBatchMutation=true;

        features.supportsTransactions=true;
        features.supportsConsistentKeyOperations=true;
        features.supportsLocking=false;

        features.isKeyOrdered=true;
        features.isDistributed=true;
        features.hasLocalKeyPartition=false;
    }

    @Override
    public FoundationDBKeyColumnValueStore openDatabase(String name) throws StorageException {
        FoundationDBKeyColumnValueStore kv = openStores.get(name);

        if (kv == null) {
            FoundationDBKeyColumnValueStore newkv = new FoundationDBKeyColumnValueStore(name, this);
            kv = openStores.putIfAbsent(name, newkv);

            if (kv == null) kv = newkv;
        }

        return kv;
    }

    @Override
    public void mutateMany(Map<String, Map<ByteBuffer, KCVMutation>> mutations, StoreTransaction txh) throws StorageException {
        for (Map.Entry<String, Map<ByteBuffer, KCVMutation>> entry: mutations.entrySet()) {
            FoundationDBKeyColumnValueStore store = openDatabase(entry.getKey());
            for (Map.Entry<ByteBuffer, KCVMutation> entryEntry : entry.getValue().entrySet()) {
                store.mutate(entryEntry.getKey(), entryEntry.getValue().getAdditions(), entryEntry.getValue().getDeletions(), txh);
            }
        }
    }

    @Override
    public StoreTransaction beginTransaction(ConsistencyLevel consistencyLevel) throws StorageException {
        Transaction tr = db.createTransaction();
        return new FoundationDBTransaction(tr);
    }

    @Override
    public void close() throws StorageException {
        openStores.clear();
        db.dispose();
    }

    @Override
    public void clearStorage() throws StorageException {
        try {
            Transaction tr = db.createTransaction();
            tr.clearRangeStartsWith(new Tuple().add(dbname).pack());
            tr.commit().get();

            close();
        }
        catch (FDBError error) {
            throw new TemporaryStorageException(error.getMessage(), error.getCause());
        }
    }

    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public String getConfigurationProperty(String key) throws StorageException {
        return null;  // todo
    }

    @Override
    public void setConfigurationProperty(String key, String value) throws StorageException {
        // todo
    }
}
