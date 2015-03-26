(ns io.sberlabs.pipeline-core-test
  (:require [clojure.test :refer :all]
            [io.sberlabs.pipeline-core :refer :all]))

(deftest hello-test
  (testing "says hello to caller"
    (is (= "Hello, foo!" (hello "foo")))))
