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
        }
        catch (FDBError error) {
            if(error.getCode() == 1007) throw new TemporaryStorageException("Transaction was open for too long.", error.getCause());
            else throw new TemporaryStorageException(error.getMessage(), error.getCause());
        }
    }

    @Override
    public void rollback() throws StorageException {
        if (tr == null) return;
        tr.reset();
    }

    @Override
    public void flush() throws StorageException {
        if (tr == null) return;
        try {
            tr.commit().get();
        }
        catch (FDBError error) {
            if(error.getCode() == 1007) throw new TemporaryStorageException("Transaction was open for too long.", error.getCause());
            else throw new TemporaryStorageException(error.getMessage(), error.getCause());
        }
    }

    @Override
    public ConsistencyLevel getConsistencyLevel() {
        return ConsistencyLevel.DEFAULT;   // todo
    }
}
