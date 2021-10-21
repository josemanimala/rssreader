import rocksdb
import hashlib
import traceback


def createoropen(path, dbname):

    opts = rocksdb.Options()
    opts.create_if_missing = True
    opts.max_open_files = 300000
    opts.write_buffer_size = 67108864
    opts.max_write_buffer_number = 3
    opts.target_file_size_base = 67108864

    opts.table_factory = rocksdb.BlockBasedTableFactory(
        filter_policy=rocksdb.BloomFilterPolicy(10),
        block_cache=rocksdb.LRUCache(2 * (1024 ** 3)),
        block_cache_compressed=rocksdb.LRUCache(500 * (1024 ** 2)),
    )

    try:
        db = rocksdb.DB(path + "/{dbname}", opts)
    except Exception as e:
        print(traceback.format_exc())
        return None

    return db


def write(db, writelist):
    try:
        batch = rocksdb.WriteBatch()
        for item in writelist:
            batch.put(hashlib.sha256(item["key"]).digest(), item["value"])
        db.write(batch)

    except Exception as e:
        print(traceback.format_exc())
        return False

    return True


def read(db, key):

    try:
        value = db.get(hashlib.sha256(key).digest())
    except Exception as e:
        print(traceback.format_exc())
        return False

    return value
