package com.thinkaurelius.titan.diskstorage.foundationdb;

import com.foundationdb.KeyValue;
import com.foundationdb.RangeQuery;
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

    @Override
    public boolean containsKey(ByteBuffer key, StoreTransaction txh) throws StorageException {
        FoundationDBTransaction tr = (FoundationDBTransaction) txh;
        return tr.get(storePrefix(Subspace.KEYS_SUBSPACE).add(key.array()).pack()) != null;
    }

    @Override
    public List<Entry> getSlice(KeySliceQuery query, StoreTransaction txh) throws StorageException {
        List<Entry> returnList = new ArrayList<Entry>();
        RangeQuery queryResult = ((FoundationDBTransaction) txh).getRange(storePrefix(Subspace.DATA_SUBSPACE).add(query.getKey().array()).add(query.getSliceStart().array()).pack(), storePrefix(Subspace.DATA_SUBSPACE).add(query.getKey().array()).add(query.getSliceEnd().array()).pack()).limit(query.getLimit());
        List<KeyValue> kvList = queryResult.asList().get();
        assert kvList.size() < query.getLimit();

        for(KeyValue kv : kvList) {
            returnList.add(new Entry(ByteBuffer.wrap(Tuple.fromBytes(kv.getKey()).getBytes(3)), ByteBuffer.wrap(kv.getValue())));
        }

        return returnList;
    }

    @Override
    public ByteBuffer get(ByteBuffer key, ByteBuffer column, StoreTransaction txh) throws StorageException {
        FoundationDBTransaction tr = (FoundationDBTransaction) txh;
        return ByteBuffer.wrap(tr.get(storePrefix(Subspace.DATA_SUBSPACE).add(key.array()).add(column.array()).pack()));
    }

    @Override
    public boolean containsKeyColumn(ByteBuffer key, ByteBuffer column, StoreTransaction txh) throws StorageException {
        FoundationDBTransaction tr = (FoundationDBTransaction) txh;
        return tr.get(storePrefix(Subspace.DATA_SUBSPACE).add(key.array()).add(column.array()).pack()) != null;
    }

    @Override
    public void mutate(ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions, StoreTransaction txh) throws StorageException {
        if (deletions != null) {
            for (ByteBuffer deleteColumn : deletions) {
                ((FoundationDBTransaction) txh).clear(storePrefix(Subspace.DATA_SUBSPACE).add(key.array()).add(deleteColumn.array()).pack());
                ((FoundationDBTransaction) txh).clear(storePrefix(Subspace.KEYS_SUBSPACE).add(key.array()).pack());
            }
        }
        if (additions != null) {
            for (Entry addColumn : additions) {
                ((FoundationDBTransaction) txh).set(storePrefix(Subspace.DATA_SUBSPACE).add(key.array()).add(addColumn.getColumn().array()).pack(), addColumn.getValue().array());
                ((FoundationDBTransaction) txh).set(storePrefix(Subspace.KEYS_SUBSPACE).add(key.array()).pack(), "".getBytes());
            }
        }
    }

    @Override
    public void acquireLock(ByteBuffer key, ByteBuffer column, ByteBuffer expectedValue, StoreTransaction txh) throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public RecordIterator<ByteBuffer> getKeys(final StoreTransaction txh) throws StorageException {
        return new RecordIterator<ByteBuffer>() {
            private final Iterator<KeyValue> results = ((FoundationDBTransaction) txh).getRangeStartsWith(storePrefix(Subspace.KEYS_SUBSPACE).pack()).iterator();

            @Override
            public boolean hasNext() throws StorageException {
                return results.hasNext();
            }

            @Override
            public ByteBuffer next() throws StorageException {
                return ByteBuffer.wrap(results.next().getKey());
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

    private Tuple storePrefix(Subspace s) {
        return new Tuple().add(dbName).add(storeName).add(s.value);
    }
}
