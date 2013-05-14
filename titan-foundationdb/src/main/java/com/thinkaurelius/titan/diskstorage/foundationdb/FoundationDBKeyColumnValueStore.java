package com.thinkaurelius.titan.diskstorage.foundationdb;

import com.foundationdb.KeySelector;
import com.foundationdb.KeyValue;
import com.foundationdb.RangeQuery;
import com.foundationdb.Transaction;
import com.foundationdb.tuple.Tuple;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FoundationDBKeyColumnValueStore implements KeyColumnValueStore {

    private final String dbName;
    private final String storeName;
    private final FoundationDBStoreManager manager;

    private static enum Subspace {

        KEYS_SUBSPACE(1),
        DATA_SUBSPACE(2);

        private Subspace(int value) {
            this.value = value;
        }

        public final int value;
     }

    public FoundationDBKeyColumnValueStore(String name, FoundationDBStoreManager m) {
        dbName = m.dbname;
        storeName = name;
        manager = m;
    }

    private byte[] getBytes(ByteBuffer b) {
        byte[] bytes = new byte[b.remaining()];
        b.get(bytes);
        b.rewind();
        return bytes;
    }

    @Override
    public boolean containsKey(ByteBuffer key, StoreTransaction txh) throws StorageException {
        return getTransaction(txh).get(storePrefix(Subspace.KEYS_SUBSPACE).add(getBytes(key)).pack()).get() != null;
    }

    @Override
    public List<Entry> getSlice(KeySliceQuery query, StoreTransaction txh) throws StorageException {
        List<Entry> returnList = new ArrayList<Entry>();
        Tuple keyPrefix = storePrefix(Subspace.DATA_SUBSPACE).add(getBytes(query.getKey()));
        RangeQuery queryResult = getTransaction(txh).getRange(KeySelector.firstGreaterOrEqual(keyPrefix.add(getBytes(query.getSliceStart())).pack()), KeySelector.firstGreaterOrEqual(keyPrefix.add(getBytes(query.getSliceEnd())).pack()));
        if (query.getLimit() > 0) queryResult = queryResult.limit(query.getLimit());
        List<KeyValue> kvList = queryResult.asList().get();

        for(KeyValue kv : kvList) {
            returnList.add(new Entry(ByteBuffer.wrap(Tuple.fromBytes(kv.getKey()).getBytes(4)), ByteBuffer.wrap(kv.getValue())));
        }

        return returnList;
    }

    @Override
    public ByteBuffer get(ByteBuffer key, ByteBuffer column, StoreTransaction txh) throws StorageException {
        byte[] result = getTransaction(txh).get(storePrefix(Subspace.DATA_SUBSPACE).add(getBytes(key)).add(getBytes(column)).pack()).get();
        if (result == null) return null;
        else return ByteBuffer.wrap(result);
    }

    @Override
    public boolean containsKeyColumn(ByteBuffer key, ByteBuffer column, StoreTransaction txh) throws StorageException {
        return getTransaction(txh).get(storePrefix(Subspace.DATA_SUBSPACE).add(getBytes(key)).add(getBytes(column)).pack()).get() != null;
    }

    @Override
    public void mutate(ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions, StoreTransaction txh) throws StorageException {
        Tuple keyPrefix = storePrefix(Subspace.DATA_SUBSPACE).add(getBytes(key));
        byte[] keyIndexKey = storePrefix(Subspace.KEYS_SUBSPACE).add(getBytes(key)).pack();

        if (deletions != null) {
            for (ByteBuffer deleteColumn : deletions) {
                getTransaction(txh).clear(keyPrefix.add(getBytes(deleteColumn)).pack());
            }
            List<KeyValue> results = getTransaction(txh).getRangeStartsWith(keyPrefix.pack()).asList().get();
            if (results.size() == 0) getTransaction(txh).clear(keyIndexKey);
        }

        if (additions != null) {
            for (Entry addColumn : additions) {
                getTransaction(txh).set(keyPrefix.add(getBytes(addColumn.getColumn())).pack(), getBytes(addColumn.getValue()));
            }
            getTransaction(txh).set(keyIndexKey, "".getBytes());
        }
    }

    @Override
    public void acquireLock(ByteBuffer key, ByteBuffer column, ByteBuffer expectedValue, StoreTransaction txh) throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public RecordIterator<ByteBuffer> getKeys(final StoreTransaction txh) throws StorageException {
        return new RecordIterator<ByteBuffer>() {
            private final Iterator<KeyValue> results = getTransaction(txh).getRangeStartsWith(storePrefix(Subspace.KEYS_SUBSPACE).pack()).iterator();

            @Override
            public boolean hasNext() throws StorageException {
                return results.hasNext();
            }

            @Override
            public ByteBuffer next() throws StorageException {
                return ByteBuffer.wrap(Tuple.fromBytes(results.next().getKey()).getBytes(3));
            }

            @Override
            public void close() throws StorageException {

            }
        };
    }

    @Override
    public ByteBuffer[] getLocalKeyPartition() throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return storeName;
    }

    @Override
    public void close() throws StorageException {
        // todo
    }

    private static Transaction getTransaction(StoreTransaction txh) {
        return ((FoundationDBTransaction) txh).getTransaction();
    }

    private Tuple storePrefix(Subspace s) {
        return new Tuple().add(dbName).add(storeName).add(s.value);
    }
}
