(ns io.sberlabs.pipeline-main
  (:require [io.sberlabs.pipeline-config :as config]
            [io.sberlabs.pipeline-core :refer :all])
  (:gen-class))

(defn -main
  [& [config-file]]
  (let [config     (config/init config-file)
        codec      (:codec config)
        store      (:store config)
        transports (:transports config)
        reactor    (reactor transports codec store (:reactor config))]
    (start! reactor)))
