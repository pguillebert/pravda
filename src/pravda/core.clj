(ns pravda.core
  (:require [s3-journal :as s3j]
            [aws.sdk.s3 :as s3]
            [taoensso.nippy :as nippy]
            [clojure.tools.logging :as log])
  (:import [java.nio ByteBuffer]
           [java.io DataInputStream InputStream]
           [org.apache.commons.compress.compressors.gzip GzipCompressorInputStream]))

(defprotocol StorableEvent
  (get-storage-path [this]
    "Returns the S3-compatible filepath where this
     StorableEvent should be written."))

(defn length-value ^bytes
  [^bytes b]
  "Creates a length-value record from a byte array.
  One record is made of two fields :
  -- Length, an integer (4 bytes) describing the byte length of the data.
  -- Data, a byte array of the aforementioned length."
  (let [l (alength b)
        ^ByteBuffer buffer (ByteBuffer/allocate (+ l 4))
        _ (.putInt buffer (int l))
        _ (.put buffer b)]
    (.array buffer)))

(defn mk-journal
  [conf spath]
  "Creates a new journal for the storage path spath."
  (s3j/journal
   {:s3-access-key (get-in conf [:s3 :access-key])
    :s3-secret-key (get-in conf [:s3 :secret-key])
    :s3-bucket (get-in conf [:s3 :bucket])
    ;; do not use dynamic path from s3-journal, hardcode our own
    :s3-directory-format (str "'" spath "'")
    :local-directory (str (:local-basedir conf) "/" spath)
    :encoder (comp length-value nippy/freeze)
    :compressor (:compressor conf)
    :delimiter "" ;; no delimiter
    :max-batch-latency (:max-batch-latency conf)
    :max-batch-size (:max-batch-size conf)
    :id (:id conf)
    :expiration (:expiration conf)}))

(def _conf_ (atom nil))

(defn close
  []
  "Closes all open journals."
  (doseq [[spath journal] (:journals @_conf_)]
    (log/info "Closing journal for" spath)
    (.close journal)))

(defn register-shutdown-hook!
  []
  "Registers a shutdown hook to close all open journals when the JVM closes."
  (.addShutdownHook
   (Runtime/getRuntime)
   (proxy [Thread] []
     (run []
       (log/info "Closing pravda")
       (try
         (close)
         (catch Exception e
           (log/error e "failed to close pravda")))))))

(defn initialize
  [conf]
  (when-not @_conf_
    (register-shutdown-hook!)
    (reset! _conf_ conf)))

(defn get-journal
  [spath]
  (if-let [existing (get-in @_conf_ [:journals spath])]
    existing
    (if-let [conf @_conf_]
      (let [new (mk-journal conf spath)]
        (swap! _conf_ assoc-in [:journals spath] new)
        new)
      (log/error "Tried to initialize a new journal"
                 "without a configuration !"))))

(defn put
  [obj]
  "Stores a StorableEvent obj in the appropriate journal.
   The object will be stored as a map with nippy."
  (when-let [j (get-journal (get-storage-path obj))]
    (s3-journal/put! j (into {} obj))))

;;; Reader

(definterface ReadableEvent [nextEvent []])

(defn make-lvis
  "Builds an input stream to read event-files"
  [^InputStream is]
  (proxy [DataInputStream ReadableEvent]
      [is]
    (nextEvent [] (let [^DataInputStream this this
                         l (proxy-super readInt)
                         b (byte-array l)
                         _ (proxy-super readFully b)]
                     (nippy/thaw b)))))

(defn build-partition
  [s3 file-path]
  (let [content-is (:content (s3/get-object s3 (:bucket s3) file-path))
        lvis (make-lvis (GzipCompressorInputStream. content-is true))]
    ;; lazy seq of all records in this file-path
    (take-while #(not= ::EOF %)
                (repeatedly (fn [] (try (.nextEvent lvis)
                                       (catch java.io.EOFException e
                                         ::EOF)))))))
