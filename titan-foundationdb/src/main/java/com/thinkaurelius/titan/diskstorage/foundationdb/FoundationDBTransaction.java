package com.thinkaurelius.titan.diskstorage.foundationdb;

import com.foundationdb.FDBError;
import com.foundationdb.KeySelector;
import com.foundationdb.RangeQuery;
import com.foundationdb.Transaction;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;

public class FoundationDBTransaction implements StoreTransaction {

    private Transaction tr;

    public FoundationDBTransaction(Transaction tr) {
        this.tr = tr;
    }

    public RangeQuery getRange(KeySelector k1, KeySelector k2) throws StorageException {
        try {
            return tr.getRange(k1, k2);
        }
        catch (FDBError error) {
            throw new TemporaryStorageException(error.getMessage(), error.getCause());
        }
    }

    public RangeQuery getRange(byte[] k1, byte[] k2) throws StorageException {
        try {
            return tr.getRange(k1, k2);
        }
        catch (FDBError error) {
            throw new TemporaryStorageException(error.getMessage(), error.getCause());
        }
    }

    public RangeQuery getRangeStartsWith(byte[] key) throws StorageException {
        try {
            return tr.getRangeStartsWith(key);
        }
        catch (FDBError error) {
            throw new TemporaryStorageException(error.getMessage(), error.getCause());
        }
    }

    public byte[] get(byte[] key) throws StorageException{
        try {
            return tr.get(key).get();
        }
        catch (FDBError error) {
            throw new TemporaryStorageException(error.getMessage(), error.getCause());
        }
    }

    public void clear(byte[] key) throws StorageException{
        try {
            tr.clear(key);
        }
        catch (FDBError error) {
            throw new TemporaryStorageException(error.getMessage(), error.getCause());
        }
    }

    public void set(byte[] key, byte[] value) throws StorageException{
        try {
            tr.set(key, value);
        }
        catch (FDBError error) {
            throw new TemporaryStorageException(error.getMessage(), error.getCause());
        }
    }

    @Override
    public void commit() throws StorageException {
        if (tr == null) return;
        try {
            tr.commit().get();
            tr = null;
        }
        catch(FDBError err) {
            throw new PermanentStorageException(err.getMessage(),err.getCause());
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
