(defproject s3-records "0.1.0-SNAPSHOT"
  :description "Stores streams of records in s3"
  :url "http://github.com/pguillebert/s3-records"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [factual/s3-journal "0.1.3"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.slf4j/slf4j-api "1.7.12"]
                 [org.slf4j/slf4j-log4j12 "1.7.12"]
                 [log4j "1.2.17"]]
  :global-vars {*warn-on-reflection* true})
