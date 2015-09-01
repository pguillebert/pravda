(ns s3-records.datalog
  (:require [s3-records.core :as core])
  (:import [java.text DateFormat SimpleDateFormat]
           [java.util Date]))

;; This is an example implementation of the StorableRecord protocol.
;; Int this example, data must provide three keys (:domain, :type, :ts)
;; key :ts will be parsed as a UNIX timestamp and will be used to store
;; records by day.

(def ^:static ^SimpleDateFormat date-format (SimpleDateFormat. "yyyy-MM-dd"))

(defrecord Datalog [domain type ^long ts]
  core/StorableRecord
  (get-storage-path [this]
    (let [d (java.util.Date. ts)
          strdate (.format date-format d)]
      (str domain "/" type "/" strdate))))
