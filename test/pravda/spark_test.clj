(ns pravda.spark-test
  (:require [pravda.spark :as spark]
            [flambo.conf :as conf]
            [flambo.api :as f]))

(def s3 (:s3 (load-file "./conf/conf.clj")))

(def c (-> (conf/spark-conf)
           (conf/master "local")
           (conf/app-name "flame_princess")))

(def sc (f/spark-context c))


;; Each file will be one spark partition.
(def data (spark/make-rdd sc s3 ["uberstein/pshb-info/2015-01-01/uniqueid-000000.journal.gz"
                                 "uberstein/pshb-info/2015-01-02/uniqueid-000000.journal.gz"
                                 "uberstein/pshb-info/2015-01-03/uniqueid-000000.journal.gz"
                                 "uberstein/pshb-info/2015-01-04/uniqueid-000000.journal.gz"
                                 "uberstein/pshb-info/2015-01-05/uniqueid-000000.journal.gz"
                                 "uberstein/pshb-info/2015-01-06/uniqueid-000000.journal.gz"
                                ]))

;; Example Flambo query
(def min-threshold 3)
(def min-period 2)

(defn detect-pshb-feeds
  [data]
  (-> data
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

       (f/take 10)))
