(ns pravda.utils
  (:import [com.amazonaws.services.s3.model
            ObjectListing S3ObjectSummary ListObjectsRequest]
           [com.amazonaws.services.s3 AmazonS3Client])
  (:require [pravda.reader :as reader]
            [clj-time.core :as time]
            [clj-time.format :as format]))

(defn date-selector
  ([format increment start end]
   "Returns a sequence of dates between DateTime `start' and `end' (inclusive).
   Interval between dates is `increment'.
   The dates are formatted as Strings using `format'."
   (let [dates (loop [dates [start]]
                 (if (or (time/after? (last dates) end)
                         (time/equal? (last dates) end))
                   dates
                   (recur (conj dates (time/plus (last dates)
                                                 increment)))))
         formatter (format/formatter format)]
     (map #(format/unparse formatter %) dates)))

  ([start end]
   "2-arity defaults to format \"yyyy-MM-dd\" and increment 1 day."
   (date-selector "yyyy-MM-dd" (time/days 1) start end))

  ([nbdays]
   "1-arity defaults to selecting nbdays of data with end = yesterday."
   (let [;; yesterday midnight (to select full day of yesterday)
         end (time/minus (time/today-at 0 0 0) (time/days 1))
         ;; nbdays before that
         start (time/minus (time/today-at 0 0 0) (time/days nbdays))]
     (date-selector "yyyy-MM-dd" (time/days 1) start end))))

(def ^AmazonS3Client s3-client (AmazonS3Client.))

(defn list-files
  [^String s3-bucket ^String path]
  "Returns the list of all file-names at a given path in AWS S3."
  (let [summaries (loop [acc []
                         obj-list (.listObjects s3-client s3-bucket path)]
                    (if (.isTruncated obj-list)
                      (recur (concat acc (.getObjectSummaries obj-list))
                             (.listNextBatchOfObjects s3-client obj-list))
                      (concat acc (.getObjectSummaries obj-list))))]
    (map (fn [^S3ObjectSummary s] (.getKey s)) summaries)))

(defn dataset-files
  [s3-bucket domain type & args]
  "Generates all file paths existing in S3, using conventions from
   Datalog implementation. Can be used with 1-arg nbdays or 2-args
   start and end dates (inclusive)."
  (->> (apply date-selector args)
       (map #(str domain "/" type "/" %))
       (mapcat #(list-files s3-bucket %))))

(defn select-latest
  [s3-bucket domain type]
  "Will pick latest date available in the directory domain/type, according
   to the Datalog specification. Returns all pravda file paths for this
   latest date. Dated directory should be lexicographically ordered."
  (let [prefix (str domain "/" type "/")
        req (-> (ListObjectsRequest.)
                (.withBucketName "linkfluence.datalog")
                (.withPrefix prefix)
                (.withDelimiter "/")
                (.withMaxKeys (int -1)))

        latest (->> (.listObjects s3-client req)
                    (.getCommonPrefixes)
                    (map #(clojure.string/split % #"/"))
                    (map last)
                    (sort)
                    (last))]
    (list-files s3-bucket (str domain "/" type "/" latest))))

(defn stream-latest
  [s3-bucket domain type]
  "Returns a lazy-seq of all the latest data for a given domain and type."
  (mapcat #(reader/build-partition s3-bucket %)
          (select-latest s3-bucket domain type)))
