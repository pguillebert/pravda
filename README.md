# pravda

A Clojure library to easily store and process a stream of unconstrained events
using Amazon S3 and Spark. Your application generates individual events
and pravda ensures they will get stored *en masse* into a meaningful directory
structure of your choice on Amazon S3. These events are then available for
processing directly into a dedicated [Spark](http://spark.apache.org) cluster.

## What is pravda

* Unconstrained, Clojure-friendly event-flow data-type
* Vertical partitioning made simple
* Simple seqable/iterable format
* Compressed storage
* Unlimited storage capacity (S3)
* Scalable computing power (Apache Spark on EC2)
* 0% Hadoop

## Anatomy of an event

A pravda event is a Clojure record implementing the protocol
`pravda.core/StorableEvent`:

    (defprotocol StorableEvent
      (get-storage-path [this]))

Your implementation of this protocol in a record will define what map keys
are expected to be present in the event. Your implementation of
`get-storage-path` uses data taken from the event map to provide pravda with
an S3-compatible filepath where this `StorableEvent` should be written.
But this structure is not contraining because records can hold more keys
than the defined ones : in other words, you can (and should) add arbitrary
additional fields in the event, they will be stored along.

A `StorableEvent` can easily be created with the default `map->Record`
constructor using a clojure map as input.

As a reference, you'll find an example implementation in the namespace
`pravda.datalog`.

## Writing to pravda

Don't forget to initialize the library with a configuration like this :

      {:s3-bucket "xxxxxxxx"
       :local-basedir "/tmp/pravda"
       :id "unique-id"
       :compressor :gzip
       :max-batch-latency (* 30 1000) ;; 30 secs
       :max-batch-size 100 ;; 100 logs
       :expiration (* 30 60 1000) ;; 30 minutes
       :flush-delay (* 3 60 1000) ;; 3 minutes
       :tidy-interval (* 60 1000) ;; 1 minute
       }

Then you can call `put` with a `StorableEvent` as many times as you wish.
The library will take care of the vertical partitioning.

The events will be secured to S3 :
* regularly,
* when the size is big enough,
* when you call `close`.

The S3 file will be compressed with the `:compressor` defined in the
configuration.

## Binary storage format

The events are stored in S3 in a very simple file format.
The file is just a concatenation of entries :

![Data format](/doc/format.png)

The byte array is the nippy serialization of your record implementing
`StorableEvent`. The size of this serialization is stored just before it as
an integer (4 bytes). This is classic Type-Length-Value storage (except there
is no type).

This format is compressed as a stream, current implementation focuses on
GZIP but others are possible.

## Reading data with Spark

Once stored into S3, the seqable property of pravda format allows the
creation of a Spark RDD. Different pravda files are mapped to the different
RDD partitions : for example if you open 7 days of data with the RDD, it'll
provide a RDD with 7 partitions. In each partition, the reader generates a
lazy-seq that is converted into a Scala iterable for Spark.

The RDD spits out `StorableEvent` records that you can manipulate with Flambo
to process the events with a Clojure-friendly DSLs in Spark.

Pravda also provides a writer function to store processed data into the
same format in S3, from your Spark application.

You'll find an example query in the test directory.

## Acknoledgements

The writer part of this library is a thin wrapper over
[s3-journal](https://github.com/Factual/s3-journal)
by Zach Tellman (at Factual)

Also heavily using [Nippy](https://github.com/ptaoussanis/nippy)
by Peter Taoussanis.

Vertical partitioning is a term tossed by Nathan Marz in his
library called [Pail](https://github.com/nathanmarz/dfs-datastores)
that heavily inspired my design.

## License

Copyright © 2015 Philippe Guillebert

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
