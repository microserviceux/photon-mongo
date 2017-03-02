(ns photon-mongo.core-test
  (:require [clojure.test :refer :all]
            [photon.db-check :as check]
            [photon.db.mongo :as mongo]
            [photon.db :as db]))

(deftest db-check-test
  (let [impl (mongo/->LocalMongoDB {:mongodb.host "127.0.0.1"})]
    (db/delete-all! impl)
    (is (true? (check/db-check impl)))))
