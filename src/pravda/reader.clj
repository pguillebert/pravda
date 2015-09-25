(ns pravda.reader
  (:require [aws.sdk.s3 :as s3]
            [taoensso.nippy :as nippy])
  (:import [java.io DataInputStream InputStream]
           [org.apache.commons.compress.compressors.gzip
            GzipCompressorInputStream]))

(definterface ReadableEventStream [nextEvent []])

(defn make-pravda-input-stream
  "Builds an input stream to read event-files"
  [^InputStream is]
  (proxy [DataInputStream ReadableEventStream]
      [is]
    (nextEvent [] (let [^DataInputStream this this
                         l (proxy-super readInt)
                         b (byte-array l)
                         _ (proxy-super readFully b)]
                     (nippy/thaw b)))))

(defn build-partition
  [s3 file-path]
  (let [content-is (:content (s3/get-object s3 (:bucket s3) file-path))
        deflated-is (GzipCompressorInputStream. content-is true)
        pravda-is (make-pravda-input-stream deflated-is)]
    ;; lazy seq of all events in this file-path
    (take-while #(not= ::EOF %)
                (repeatedly (fn [] (try (.nextEvent pravda-is)
                                       (catch java.io.EOFException e
                                         ::EOF)))))))
