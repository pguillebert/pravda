(ns pravda.datalog
  (:require [pravda.writer :as writer])
  (:import [java.text DateFormat SimpleDateFormat]
           [java.util Date]))

;; This is an example implementation of the StorableEvent protocol.
;; Int this example, data must provide three keys (:domain, :type, :ts)
;; key :ts will be parsed as a UNIX timestamp and will be used to store
;; events one directory by day.

(def ^:static ^SimpleDateFormat date-format (SimpleDateFormat. "yyyy-MM-dd"))

(defrecord Datalog [domain type ^long ts]
  writer/StorableEvent
  (get-storage-path [this]
    (let [d (java.util.Date. ts)
          strdate (.format date-format d)]
      (str domain "/" type "/" strdate))))
