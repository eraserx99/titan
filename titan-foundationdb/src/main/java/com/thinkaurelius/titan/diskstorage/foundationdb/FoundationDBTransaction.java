package com.thinkaurelius.titan.diskstorage.foundationdb;

import com.foundationdb.FDBError;
import com.foundationdb.Transaction;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;

public class FoundationDBTransaction implements StoreTransaction {

    private Transaction tr;

    public FoundationDBTransaction(Transaction tr) {
        this.tr = tr;
    }

    public Transaction getTransaction() {
        return tr;
    }


    @Override
    public void commit() throws StorageException {
        if (tr == null) return;
        try {
            tr.commit().get();
            tr = null;
        }
        catch (FDBError error) {
            throw new TemporaryStorageException(error.getMessage(), error.getCause());
        }
    }

    @Override
    public void rollback() throws StorageException {
        if (tr == null) return;
        tr.reset();
        tr = null;
    }

    @Override
    public void flush() throws StorageException {
                      // todo
    }

    @Override
    public ConsistencyLevel getConsistencyLevel() {
        return ConsistencyLevel.DEFAULT;   // todo
    }
}
