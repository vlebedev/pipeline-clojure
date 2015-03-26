(ns io.sberlabs.pipeline-stores-kafka
  (:require [clojure.core.async :as a :refer [<! >! go go-loop]]
            [io.sberlabs.pipeline-core :refer [Store Service]]
            [io.sberlabs.pipeline-kafka :as kafka]))

(defn kafka-batching-store
  [{:keys [proxy-url schema-registry-url topic
           schema batch-size timeout chan-sliding-buffer-size] :as config}]
  (let [schema-id (kafka/get-schema-id-or-register schema-registry-url topic schema)
        partitions-number (kafka/get-partitions-number proxy-url topic)
        extended-config (-> config
                            (assoc :schema-id schema-id)
                            (assoc :partitions-number partitions-number)
                            (assoc :record-partitoner kafka/guava-consistent-hash-partitioner))
        sink (a/chan (a/sliding-buffer chan-sliding-buffer-size))]
    (reify
      Store
      (store! [this payload]
        (go (>! sink payload)))
      Service
      (start! [this]
        (go-loop []
          (when-let [msg (<! sink)]
            (loop [batch [msg]
                   cnt batch-size
                   timeout-ch (a/timeout timeout)]
              (let [[msg ch] (a/alts! [sink timeout-ch])]
                (if (= ch sink)
                  (if (= cnt 0)
                    (kafka/produce-batch extended-config (conj batch msg))
                    (recur (conj batch msg) (dec cnt) timeout-ch))
                  (kafka/produce-batch extended-config batch))))
            (recur)))))))
