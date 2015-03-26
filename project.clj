(defproject io.sberlabs/pipeline "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [slingshot "0.12.2"]
                 [cheshire "5.4.0"]
                 [http-kit "2.1.16"]
                 [biscuit "1.0.0"]
                 [org.zeromq/cljzmq "0.1.4"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [clj-yaml "0.4.0"]
                 [com.google.guava/guava "18.0"]]

  :profiles {:dev  {:resource-paths ["config/dev"]}
             :prod {:resource-paths ["config/prod"]}}

  :jvm-opts ["-Djava.library.path=/usr/lib:/usr/local/lib"]

  :main io.sberlabs.pipeline-main

  :aot [io.sberlabs.pipeline-main
        io.sberlabs.pipeline-transports-zmq
        io.sberlabs.pipeline-codecs-rutarget
        io.sberlabs.pipeline-stores-kafka])
