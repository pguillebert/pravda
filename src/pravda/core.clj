(ns pravda.core
  (:require [s3-journal :as s3j]
            [aws.sdk.s3 :as s3]
            [taoensso.nippy :as nippy]
            [clojure.tools.logging :as log])
  (:import [java.nio ByteBuffer]
           [java.util.concurrent TimeUnit Executors ScheduledExecutorService]
           [java.io DataInputStream InputStream]
           [org.apache.commons.compress.compressors.gzip
            GzipCompressorInputStream]))

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
(def _journals_ (atom nil))
(def _timers_ (atom nil))

(defn get-journal
  [spath]
  (locking _journals_
    (if-let [existing (get @_journals_ spath)]
      existing
      (if-let [conf @_conf_]
        (let [new (mk-journal conf spath)]
          (swap! _journals_ assoc spath new)
          new)
        (log/error "Tried to initialize a new journal"
                   "without a configuration !")))))

(defn close-journal
  [spath]
  (locking _journals_
    (if-let [existing (get @_journals_ spath)]
      (do (.close existing)
          (swap! _conf_ update-in [:journals] dissoc spath))
      (log/error "Cannot close non-existing journal" spath))))

(defn close-all
  []
  "Closes all open journals."
  (doseq [[spath journal] @_journals_]
    (log/info "Closing journal for" spath)
    (close-journal spath)))

(defn register-shutdown-hook!
  []
  "Registers a shutdown hook to close all open journals when the JVM closes."
  (.addShutdownHook
   (Runtime/getRuntime)
   (proxy [Thread] []
     (run []
       (log/info "Closing pravda")
       (try
         (close-all)
         (catch Exception e
           (log/error e "failed to close pravda")))))))

(defn now [] (System/currentTimeMillis))

(def ^ScheduledExecutorService _scheduler_ (Executors/newScheduledThreadPool 1))

(defn flush-journals
  [current-timers flush-delay]
  (log/info "Starting tidy loop")
  (->> current-timers
       (map (fn [[storage-path time]]
              (if (> (now) (+ time flush-delay))
                (do
                  (log/info "TIDY: flushing" storage-path
                            " because it is inactive since" time)
                  (close-journal storage-path)
                  ;; remove this timer from list
                  nil)
                ;; else keep this timer running
                [storage-path time])))
       (filter identity)
       (doall)
       (into {})))

(defn start-journal-tidy!
  [{:keys [flush-delay tidy-interval] :as conf}]
  "Setups a thread to flush (close) journals not written for flush-delay.
   This task runs every tidy-interval."
  (.scheduleAtFixedRate _scheduler_ #(swap! _timers_ flush-journals flush-delay)
                        tidy-interval
                        tidy-interval
                        TimeUnit/MILLISECONDS))

(defn initialize
  [conf]
  "This is used to provide pravda with a configuration. May
  be used multiple times but only the first call will be honored."
  (locking _conf_
    (when-not @_conf_
      (register-shutdown-hook!)
      (start-journal-tidy! conf)
      (reset! _conf_ conf))))

(defn put
  [obj]
  "Stores a StorableEvent obj in the appropriate journal.
   The object will be stored as a map with nippy."
  (when-let [storage-path (get-storage-path obj)]
    (when-let [journal (get-journal storage-path)]
      (swap! _timers_ assoc storage-path (now))
      (s3-journal/put! journal (into {} obj)))))

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
