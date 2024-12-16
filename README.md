## LeveDB python bitcoin tools
This is a set of tools that I hacked together to work-around some limmitations of the single-threaded [LevelDB](https://github.com/google/leveldb) library.
The initial intent was to create an efficient graph sampler for BTC blockchain data, without setting up a full node and without relying on limmited APIs.
The data to be used (without relying on setting up a node) can be found on [https://chartalist.org](https://chartalist.org) and need to be downloaded manually. 
Note that there is already a library that claims to be essentially a multi-threaded well-engineered levelDB alternative called [RocksDB](https://rocksdb.org/) but its python bindings are brocken. 

Therefore, I set up a crude map-reduce type of setup to first create a set of many smaller LeveDB databases containing the data I need for my analyses, 
and then merging them (reduce). There are several improvements to be applyied after this first version, and most notably, some convenience functions to trace the bitcoin transaction graph efficiently.
In the future I may re-write this for use with the popular Bitcoin ETL library.

