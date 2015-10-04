(ns pravda.writer-test
  (:require [clojure.test :refer :all]
            [pravda.writer :as writer]
            [pravda.datalog :as dlog]))

(deftest writer-test
  (let [amap {:domain "dd" :type "ttt" :ts 687327217127
              :data "sqsfqsf" :didi "ooo"}
        arecord (dlog/map->Datalog amap)]
    (writer/initialize (load-file "./conf/conf.clj"))
    (writer/put arecord)
    ;; (core/close-all)
    ))
