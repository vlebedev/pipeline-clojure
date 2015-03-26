(ns io.sberlabs.pipeline-config)

(defn find-ns-var
  [candidate]
  (try
    (let [var-in-ns  (symbol candidate)
          ns         (symbol (namespace var-in-ns))]
      (require ns)
      (find-var var-in-ns))
    (catch Exception _)))

(defn instantiate
  [candidate config]
  (if-let [reifier (find-ns-var candidate)]
    (reifier config)
    (throw (ex-info (str "no such var: " candidate) {}))))

(defn get-instance
  [config]
  (let [candidate (-> config :use name symbol)
        raw-config (dissoc config :use)]
    (instantiate candidate raw-config)))

(defn load-path
  [path]
  (-> (or path
          (System/getenv "PIPELINE_CONF_PATH")
          "/etc/pipeline_default.yaml")
      slurp
      read-string))

(defn get-transports
  [transports]
  (zipmap (keys transports)
          (mapv get-instance (vals transports))))

(defn init
  [path]
  (try
    (-> (load-path path)
        (update-in [:codec] get-instance)
        (update-in [:store] get-instance)
        (update-in [:transports] get-transports))))
