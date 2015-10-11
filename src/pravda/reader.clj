(ns pravda.reader
  (:require [taoensso.nippy :as nippy])
  (:import [java.io DataInputStream InputStream]
           [org.apache.commons.compress.compressors.gzip
            GzipCompressorInputStream]
           [com.amazonaws.auth DefaultAWSCredentialsProviderChain]
           [com.amazonaws.services.s3 AmazonS3Client]))

;; This interface defines a method to read the pravda file
;; event by event.
(definterface ReadableEventStream
  (nextEvent []))

;; This defines the AWS S3 client. Uses the default credentials
;; provider chain.
(def ^AmazonS3Client s3-client
  (AmazonS3Client. (DefaultAWSCredentialsProviderChain.)))

(defn build-partition
  [^String s3-bucket ^String file-path]
  "Returns a lazy-seq of Events for one pravda file located
   by bucket name and file-path."
  (let [obj (.getObject s3-client s3-bucket file-path)
        content-is (.getObjectContent obj)
        deflated-is (GzipCompressorInputStream. content-is true)
        ;; Define a proxy extending this input stream, adding the
        ;; nextEvent method by implementing ReadableEventStream.
        pravda-is (proxy [DataInputStream ReadableEventStream]
                      [deflated-is]
                    (nextEvent [] (let [^DataInputStream this this
                                        l (proxy-super readInt)
                                        b (byte-array l)
                                        _ (proxy-super readFully b)]
                                    (nippy/thaw b))))]
    ;; return a lazy-seq of all events in this file-path until the EOF.
    (take-while #(not= ::EOF %)
                (repeatedly (fn [] (try (.nextEvent pravda-is)
                                       (catch java.io.EOFException e
                                         ::EOF)))))))
