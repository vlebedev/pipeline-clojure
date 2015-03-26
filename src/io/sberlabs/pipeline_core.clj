(ns io.sberlabs.pipeline-core
  (:require [clojure.core.async :refer [>!! <! go-loop] :as a]
            [cheshire.core :as json]))

(defprotocol Store
  (store! [this payload]))

(defprotocol Transport
  (listen! [this sink]))

(defprotocol Codec
  (decode [this payload]))

(defprotocol Service
  (start! [this]))

(defn reactor
  [transports codec store config]
  (let [ch (a/chan (a/sliding-buffer (config :chan-sliding-buffer-size)))]
    (reify Service
      (start! [this]
        (go-loop []
          (when-let [msg (<! ch)]
            (store! store (decode codec msg))
            (recur)))
        (start! store)
        (doseq [[_ transport] transports]
          (start! transport)
          (listen! transport ch))))))

(defn edn-codec [config]
  (reify Codec
    (decode [this payload]
      (read-string payload))))

(defn json-codec [config]
  (reify Codec
    (decode [this payload]
      (json/parse-string payload))))

(defn stdout-store [config]
  (reify Store
    (store! [this payload]
      (println "storing: " payload))))

(defn stdin-transport [config]
  (let [sink (atom nil)]
    (reify
      Transport
      (listen! [this new-sink]
        (reset! sink new-sink))
      Service
      (start! [this]
        (future
          (loop []
            (when-let [input (read-line)]
              (>!! @sink input)
              (recur))))))))
