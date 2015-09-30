(ns pravda.spark
  (:require [pravda.reader :as reader]
            [pravda.writer :as writer]
            [flambo.api :as f]
            [flambo.function :as func])
  (:import [org.apache.spark Partition SparkContext TaskContext]))

(defrecord FilePartition
    [index file]
  ;; implement the spark Partition interface
  Partition
  (index [this] index))

(defn make-rdd
  [^org.apache.spark.api.java.JavaSparkContext sc s3 files]
  (let [sc (.sc sc)
        scala-nil scala.collection.immutable.Nil$/MODULE$
        classtag (scala.reflect.ClassTag$/MODULE$)
        clojure-classtag (.apply classtag clojure.lang.APersistentMap)
        ;; create a Spark RDD to source our events
        rdd (proxy [org.apache.spark.rdd.RDD] [sc scala-nil clojure-classtag]

              (compute [^Partition split-in ^TaskContext _]
                (let [^FilePartition split-in split-in
                      file (.file split-in)
                      ^java.util.Collection part (reader/build-partition s3 file)
                      java-conversions (scala.collection.JavaConversions$/MODULE$)]
                  (.asScalaIterator java-conversions (.iterator part))))

              (getPartitions []
                (->> files
                     (map-indexed (fn [index file] (->FilePartition index file)))
                     (into-array))))]
    ;; convert to a JavaRDD
    (org.apache.spark.api.java.JavaRDD. rdd clojure-classtag)))

(defn store-rdd
  [^org.apache.spark.api.java.JavaRDD rdd conf identifier constructor]
  (-> rdd
      (f/map-partitions-with-index
       (f/fn [idx iterator]
         ;; initialize a pravda writer
         (writer/initialize (assoc conf :id (str identifier idx)) )

         (clojure.tools.logging/warn "Starting store of partition" idx)
         ;; iterate all the rdd and store each event

         (doseq [iter (iterator-seq iterator) ]
           (writer/put (constructor iter)))
         (clojure.tools.logging/warn "Finished store of partition" idx)
         ;; return an iterator
         (.iterator (seq [idx]))))
      ;; force realization of storage
      (f/collect))
  ;; close the journals
  (writer/close-all))
