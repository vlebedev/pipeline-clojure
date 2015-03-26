(ns io.sberlabs.pipeline-kafka
  (:require [clojure.core.async :as a :refer [<! >!]]
            [org.httpkit.client :as http]
            [cheshire.core :as json]
            [slingshot.slingshot :refer [throw+]]
            [biscuit.core :refer [crc32]])
  (:import  [com.google.common.hash Hashing]))

(defn register-schema-version
  [schema-registry-url subject schema]
  (let [schema-json (json/generate-string {:schema (json/generate-string schema)})
        options {:accept "application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json"
                 :content-type "application/vnd.schemaregistry.v1+json"
                 :body schema-json}
        {:keys [status headers body error] :as resp}
        @(http/post (str schema-registry-url "/subjects/" subject "/versions") options)]
    (println schema-registry-url "\n" subject "\n" schema-json)
    (if (= status 200)
      (:id (json/parse-string body true))
      (throw+ {:type ::register-schema-version-failed :error error :status status :body body}))))

(defn get-schema-id
  [schema-registry-url subject schema]
  (let [options {:accept "application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json"
                 :content-type "application/vnd.schemaregistry.v1+json"
                 :body (json/generate-string {:schema (json/generate-string schema)})}
        {:keys [status headers body error] :as resp}
        @(http/post (str schema-registry-url "/subjects/" subject) options)]
    (if (= status 200)
      (:id (json/parse-string body true))
      nil)))

(defn get-schema-id-or-register
  [schema-registry-url subject schema]
  (or (get-schema-id schema-registry-url subject schema)
      (register-schema-version schema-registry-url subject schema)))

(defn get-partitions-number
  [proxy-url topic]
  (let [options {:accept "application/vnd.kafka.v1+json, application/vnd.kafka+json, application/json"}
        {:keys [status headers body error] :as resp}
        @(http/get (str proxy-url "/topics/" topic "/partitions") options)]
    (if (= status 200)
      (count (json/parse-string body))
      (throw+ {:type ::proxy-get-topic-partitions-failed :topic topic :status status}))))

(defn guava-consistent-hash-partitioner
  [record key buckets]
  (Hashing/consistentHash (crc32 (record key)) buckets))

(defn partition-batch
  [{:keys [partitions-number record-partitioner]} batch]
  (map #(assoc % :partition (record-partitioner (% :key))) batch))

(defn produce-batch
  [{:keys [proxy-url topic schema-id record-partitioner] :as config} batch]
  (let [body {:value_schema_id schema-id
              :records (partition-batch config batch)}
        options {:content-type "application/vnd.kafka.avro.v1+json"
                 :accept "application/vnd.kafka.v1+json, application/vnd.kafka+json, application/json"
                 :body body}
         {:keys [status headers body error] :as resp}
        @(http/post (str proxy-url "/topics/" topic) options)]
    (if (= status 200)
      (json/parse-string body)
      (throw+ {:type ::kafka-rest-proxy-producer-failed :error error :status status :body body}))))

;; OLD STUFF ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def consumer-register-string
  (json/generate-string {:format "avro"
                         :auto.offset.reset "smallest"
                         :auto.commit.enable "true"}))

(defn config-consumer
  [{:keys [proxy-url topic consumer-group]}]
  (let [options {:accept "application/vnd.kafka.v1+json, application/vnd.kafka+json, application/json"
                 :body consumer-register-string}
        {:keys [status headers body error] :as resp}
        @(http/post (str proxy-url "/consumers/" consumer-group) options)
        {:keys [base_uri instance_id]}
        (if (= status 200)
          (json/parse-string body true)
          (throw+ {:type ::consumer-registration-failed :error error :status status :body body}))]
    {:proxy-url proxy-url
     :topic topic
     :consumer-group consumer-group
     :base-uri base_uri
     :instance-id instance_id}))

(defn start-consumer
  [{:keys [base-uri topic] :as config} out]
  (a/go-loop []
    (let [options {:accept "application/vnd.kafka.avro.v1+json"}
          {:keys [status headers body error] :as resp}
          @(http/get (str base-uri "/topics/" topic) options)]
      (if (= status 200)
        (>! out (json/parse-string body))
        (throw+ {:type ::consumer-get-records-failed :error error :status status :body body}))
      (recur))))


