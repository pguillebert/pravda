(ns s3-records.core-test
  (:import [java.io DataInputStream File FileInputStream])
  (:require [clojure.test :refer :all]
            [s3-records.core :as core]
            [s3-records.datalog :as dlog]))

(deftest write-test
  (let [amap {:domain "dd" :type "ttt" :ts 687327217127
              :data "sqsfqsf" :didi "ooo"}
        arecord (dlog/map->Datalog amap)]
    (core/initialize (load-file "./conf/conf.clj"))
    (core/put arecord)
    (core/close)))

(definterface RecordReadable [^bytes nextRecord []])

(defn lvis
  [^String fname]
  (proxy [java.io.DataInputStream RecordReadable]
      [(FileInputStream. (File. fname))]
    (nextRecord [] (let [l (proxy-super readInt)
                         b (byte-array l)
                         _ (proxy-super readFully b)]
                     b))))
