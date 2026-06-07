## KV Store Roadmap

This project is currently a hash-indexed key-value store with a write-ahead log and append-only persistence.
The next goal is to evolve it into a more durable and scalable storage engine, most likely following an LSM-style design.

### Current Direction

- in-memory primary index: `key -> file offset`
- active in-memory write buffer
- write-ahead log for durability
- append-only on-disk data file

### Next Steps

- [x] improve error handling
- [x] add WAL recovery on startup
- [x] refactor the different parts
- [x] define a durable record format with lengths and tombstones
- [x] flush the in-memory buffer to sorted SSTable-style files
- [ ] add compaction
- [ ] add compression
- [ ] add secondary indexes
- [ ] add security layers
- [ ] convert the hashtable of the main index in the store, to a balanced tree

### Notes

- The current architecture is closer to an LSM path than a B+ tree path.
- A hash index is still an index; it is just optimized for exact lookups rather than ordered scans.
- Secondary indexes can be added later once record structure and recovery are more stable.
