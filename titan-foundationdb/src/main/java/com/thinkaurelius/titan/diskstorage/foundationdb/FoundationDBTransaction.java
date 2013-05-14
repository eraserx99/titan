package com.thinkaurelius.titan.diskstorage.foundationdb;

import com.foundationdb.FDBError;
import com.foundationdb.Transaction;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;

public class FoundationDBTransaction implements StoreTransaction {

    private Transaction tr;
    private FoundationDBStoreManager manager;

    public FoundationDBTransaction(Transaction tr, FoundationDBStoreManager manager) {
        this.tr = tr;
        this.manager = manager;
    }

    public Transaction getTransaction() {
        return tr;
    }


    @Override
    public void commit() throws StorageException {
        if (tr == null) return;
        try {
            tr.commit().get();
            tr = manager.createDbTransaction();
        }
        catch (FDBError error) {
            throw new TemporaryStorageException(error.getMessage(), error.getCause());
        }
    }

    @Override
    public void rollback() throws StorageException {
        if (tr == null) return;
        tr.reset();
        tr = manager.createDbTransaction();
    }

    @Override
    public void flush() throws StorageException {
        if (tr == null) return;
        try {
            tr.commit().get();
            tr = manager.createDbTransaction();
        }
        catch (FDBError error) {
            throw new TemporaryStorageException(error.getMessage(), error.getCause());
        }
    }

    @Override
    public ConsistencyLevel getConsistencyLevel() {
        return ConsistencyLevel.DEFAULT;   // todo
    }
}
