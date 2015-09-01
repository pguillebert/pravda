# s3-records

A Clojure library to easily store a stream of unconstrained, unstructured events
in Amazon S3. You provide individual events and they will get stored *en masse*
into a meaningful directory structure of your choice on Amazon S3.

An event is a Clojure record implementing the protocol `s3-records.core/StorableEvent`:

    (defprotocol StorableEvent
      (get-storage-path [this]))

`get-storage-path` uses data taken from the event to provide a S3-compatible filepath
where this `StorableEvent` should be written. You'll find an example implementation
in the namespace `s3-records.datalog`.

Your implementation defines the mandatory fields used in your `get-storage-path`.
Events can be generated with the `map->Datalog` constructor using a clojure map
containing these fields. You can add arbitrary additional fields in the map
that will be stored along.

Don't forget to initialize the library with a configuration like this :

      {:s3 {:access-key "xxxxxxxxxxxxxxxxx"
            :secret-key "xxxxxxxxxxxxxxxxxxxxxxxx"
            :bucket "xxxxxxxx"}
       :local-basedir "/tmp/s3-records"
       :id "unique-id"
       :compressor :snappy
       :max-batch-latency (* 30 1000) ;; 30 secs
       :max-batch-size 100 ;; 100 logs
       :expiration (* 30 60 1000) ;; 30 minutes
       }

All data is flushed on S3 when you call `close` in the core namespace.

## Binary storage format

The events are stored in S3 in a very simple file format.
The file is just a concatenation of entries :

          Entry:
            Size of Nippy data (int)
            Nippy data (byte array)

The byte array is the nippy representation of the clojure map equivalent
of the provided `StorableEvent`.

Once the file is pushed on S3 (when the size is big enough, or when closing),
it'll be compressed with the `:compressor` defined in the configuration.

## TODO

* Implement the reader part for various readers (cascading, spark)
* ensure there is no race conditions in mutable stuff
* auto flush events to S3 in a configurable way

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
