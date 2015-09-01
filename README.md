# s3-records

A Clojure library to easily store a stream of unconstrained events in Amazon S3.
You spit individual events and they will get stored *en masse* into a meaningful directory
structure on Amazon S3.

An event is a Clojure record implementing the protocol `s3-records.core/StorableRecord`:

    (defprotocol StorableRecord
      (get-storage-path [this]))

`get-storage-path` uses data taken from the record to provide a S3-compatible filepath
where this `StorableRecord` should be written. You'll find an example implementation
in the namespace `s3-records.datalog`.

Your implementation should use the `map->Datalog` constructor with a clojure map
containing the mandatory fields used in your `get-storage-path`. You can add arbitrary
additional fields in the map that will be stored along.

Don't forget to initialize with a conf like this :

    (def conf
      {:s3 {:access-key "xxxxxxxxxxxxxxxxx"
            :secret-key "xxxxxxxxxxxxxxxxxxxxxxxx"
            :bucket "xxxxxxxx"}
       :local-basedir "/tmp/s3-records"
       :id "unique-id"
       :compressor :snappy
       :max-batch-latency (* 30 1000) ;; 30 secs
       :max-batch-size 100 ;; 100 logs
       :expiration (* 30 60 1000) ;; 30 minutes
       })

All data is flushed on S3 when you call `close` on the

## Binary storage format

The events are stored in S3 in a very simple file format.
The file is just a concatenation of entries :

          Entry:
            Size of record (int)
            Nippy data (byte array)

The byte array is the nippy representation of the clojure map equivalent
of the provided `StorableRecord`.
Once the file is pushed on S3 (when the size is big enough, or when closing),
it'll be compressed with the provided `:compressor`.

## TODO

* Implement the reader part for various readers (cascading, spark)

## Acknoledgements

This library is a thin wrapper over
[s3-journal](https://github.com/Factual/s3-journal)
by Zach Tellman (at Factual)

Also heavily using [Nippy](https://github.com/ptaoussanis/nippy)
by Peter Taoussanis.

## License

Copyright © 2015 Philippe Guillebert

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
