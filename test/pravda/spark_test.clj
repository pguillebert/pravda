(ns pravda.spark-test
  (:require [clojure.test :refer :all]
            [clj-time.core :as time]
            [pravda.utils :as utils]
            [pravda.spark :as spark]
            [pravda.datalog :as dlog]
            [flambo.conf :as conf]
            [flambo.api :as f]))

(def conf (load-file "./conf/conf.clj"))

(def c (-> (conf/spark-conf)
           (conf/master "local")
           (conf/app-name "flame_princess")))

(def sc (f/spark-context c))
(def s3-bucket (:s3-bucket conf))

;; Each file will be one spark partition.
(def data (spark/make-rdd sc s3-bucket
                          (utils/dataset-files
                           s3-bucket "uberstein" "pshb-info"
                           (time/date-time 2015 01 01)
                           (time/date-time 2015 01 06))))

;; Example Flambo query
(def min-threshold 3)
(def min-period 2)

(defn detect-pshb-feeds
  [data]
  (-> data
      (f/map (f/fn [storable] (into {} storable)))
      (f/group-by (f/fn [{:keys [url hub topic]}]
                    [url hub topic]))

       (f/map (f/fn [^scala.Tuple2 tuple]
                (let [[url hub topic] (._1 tuple) ;; group key
                      group (._2 tuple)
                      vals (map :ts group)
                      min-val (apply min vals)
                      max-val (apply max vals)
                      deltadays (int (/ (/ (- max-val min-val) 1000) 86400))
                      cnt (count group)]

                  {:url url :hub hub :topic topic
                   :minv min-val :maxv max-val
                   :delta deltadays :cnt cnt})))

       ;; must have enough significant entries
       (f/filter (f/fn [{:keys [cnt delta]}]
                   (and (> cnt min-threshold)
                        (> delta min-period))))

       (f/map (f/fn [e]
                (assoc e :domain "processed"
                       :type "pshb-feeds"
                       :ts (System/currentTimeMillis))))

       ;; Store using a datalog structure
       (spark/store-rdd conf "testoo" dlog/map->Datalog)))

(deftest spark-test
  (detect-pshb-feeds data))
