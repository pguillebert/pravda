(ns pravda.core-test
  (:import [java.io DataInputStream File FileInputStream])
  (:require [clojure.test :refer :all]
            [pravda.core :as core]
            [pravda.datalog :as dlog]))

(deftest write-test
  (let [amap {:domain "dd" :type "ttt" :ts 687327217127
              :data "sqsfqsf" :didi "ooo"}
        arecord (dlog/map->Datalog amap)]
    (core/initialize (load-file "./conf/conf.clj"))
    (core/put arecord)
    ;; (core/close-all)
    ))

(definterface RecordReadable [^bytes nextRecord []])

(defn lvis
  [^String fname]
  (proxy [java.io.DataInputStream RecordReadable]
      [(FileInputStream. (File. fname))]
    (nextRecord [] (let [l (proxy-super readInt)
                         b (byte-array l)
                         _ (proxy-super readFully b)]
                     b))))
