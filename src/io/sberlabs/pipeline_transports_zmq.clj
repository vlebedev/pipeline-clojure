(ns io.sberlabs.pipeline-transports-zmq
  (:require [clojure.core.async :refer [>!!]]
            [clojure.tools.logging :as log]
            [zeromq.zmq :as z]
            [io.sberlabs.pipeline-core :refer [Transport Service]]))

(defn zmq-sub-transport
  "Zeromq PUB/SUB listener"
  [{:keys [addr topic]}]
  (let [sink (atom nil)]
    (reify
      Transport
      (listen! [this new-sink]
        (log/info "zmq-sub-transport: registering new listener")
        (reset! sink new-sink))
      Service
      (start! [this]
        (future
          (let [context (z/zcontext)
                poller (z/poller context 1)]
            (with-open [subscriber (doto (z/socket context :sub)
                                     (z/connect addr)
                                     (z/subscribe topic))]
              (z/register poller subscriber :pollin)
              (log/info (format "zmq-sub-transport: listener has been started for %s, topic: %s" addr topic))
              (loop []
                (z/poll poller)
                (when (z/check-poller poller 0 :pollin)
                  (let [[identity content] (z/receive-all subscriber)]
                    (>!! @sink (String. content))
                    (recur)))))))))))

