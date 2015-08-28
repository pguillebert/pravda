(ns s3-records.datalog
  (:require [s3-records.core :as core])
  (:import [java.text DateFormat SimpleDateFormat]
           [java.util Date]))

(def ^:static ^SimpleDateFormat date-format (SimpleDateFormat. "yyyy-MM-dd"))

(deftype Datalog [map]
  core/StorableRecord
  (get-storage-path [this]
    (let [{:keys [domain type ts] :as o} (map this)
          d (java.util.Date. ts)
          strdate (.format date-format d)]
      (str domain "/" type "/" strdate)))
  (as-map [this] (map this)))
