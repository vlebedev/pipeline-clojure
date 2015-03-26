(ns io.sberlabs.pipeline-codecs-rutarget
  (:require [cheshire.core :as json]
            [io.sberlabs.pipeline-core :refer [Codec]]))

(defn- record-fixer
  [record]
  (let [ip-trunc (record :ip_trunc)]
    (if (nil? ip-trunc)
      (assoc record :ip_trunc false)
      record)))

(defn rutarget-codec [config]
  (reify Codec
    (decode [this payload]
      (let [record (json/parse-string payload true)
            id (record :id)]
        (if (nil? id)
          nil
          {:key id
           :value (record-fixer record)})))))

