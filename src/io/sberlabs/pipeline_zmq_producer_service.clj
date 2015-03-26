(ns io.sberlabs.pipeline-zmq-producer-service
  (:require [clojure.core.async :as a]
            [clojure.tools.logging :as log]
            [edn-config.core :refer [env]]
            [io.sberlabs.pipeline-kafka :as k]
            [io.sberlabs.pipeline-zmq :as z]))

(defn- rutarget-parser
  [record]
  (let [ip-trunc (record :ip_trunc)]
    (if (nil? ip-trunc)
      (assoc record :ip_trunc false)
      record)))

(defn start-zmq-producer
  []
  (log/info "Starting rutarget clickstream producer service")
  (let [cfg (-> (get-in env [:kafka-producer :profiles])
                (assoc :record-partititoner k/guava-consistent-hash-partitioner)
                (assoc :record-parser rutarget-parser))
        producer (k/config-producer cfg)
        ch (a/chan (a/sliding-buffer (get-in env [:zmq :chan-size])))
        addr (get-in env [:zmq :addr])
        topic (get-in env [:zmq :topic])]
    (k/start-producer producer ch)
    (z/start-consumer addr topic ch)))

