(ns pravda.spark
  (:require [pravda.reader :as reader]
            [pravda.writer :as writer]
            [flambo.api :as f]
            [flambo.function :as func])
  (:import [org.apache.spark Partition SparkContext TaskContext]))

(defrecord FilePartition [index file]
  ;; implement the spark Partition interface
  Partition
  (index [this] index))

(defn make-rdd
  [^org.apache.spark.api.java.JavaSparkContext sc s3-bucket files]
  (let [sc (.sc sc)
        ;; the scala "nil" class
        scala-nil scala.collection.immutable.Nil$/MODULE$
        ;; The scala classtag for our StorableEvent type
        storable-classtag (.apply (scala.reflect.ClassTag$/MODULE$)
                                  pravda.writer.StorableEvent)
        ;; create a Spark RDD to source our events
        rdd (proxy [org.apache.spark.rdd.RDD]
                [sc scala-nil storable-classtag]

              ;; This method describes all the partitions available
              ;; in this RDD as an array of FilePartition.
              (getPartitions []
                (->> files
                     (map-indexed (fn [index file]
                                    (->FilePartition index file)))
                     (into-array)))

              ;; This method returns the actual content for a partition
              (compute [^Partition split-in ^TaskContext _]
                (let [^FilePartition split-in split-in
                      file (.file split-in)
                      j-conv (scala.collection.JavaConversions$/MODULE$)
                      ;; build the lazy-seq of events
                      ^java.util.Collection part
                        (reader/build-partition s3-bucket file)]
                  ;; return the equivalent scala iterator on the partition
                  (.asScalaIterator j-conv (.iterator part)))))]

    ;; Finally convert the spark RDD to a JavaRDD for Flambo
    (org.apache.spark.api.java.JavaRDD. rdd storable-classtag)))

(defn store-rdd
  [^org.apache.spark.api.java.JavaRDD rdd conf identifier]
  (-> rdd
      (f/map-partitions-with-index
       (f/fn [idx iterator]
         ;; initialize a pravda writer
         (writer/initialize (assoc conf :id (str identifier idx)) )

         (clojure.tools.logging/warn "Starting store of partition" idx)
         ;; iterate all the rdd and store each event

         (doseq [item (iterator-seq iterator)]
           (writer/put item))
         (clojure.tools.logging/warn "Finished store of partition" idx)
         ;; return an descriptive iterator with the partition just stored
         (.iterator (seq [idx]))))
      ;; force realization of storage
      (f/collect))
  ;; close the journals
  (writer/close-all))
