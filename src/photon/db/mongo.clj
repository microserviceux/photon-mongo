(ns photon.db.mongo
  (:require [photon.db :as db]
            [clojure.tools.logging :as log]
            [somnium.congomongo :as m]))

(def page-size 100)

(defn mongo-conn [conf]
  (m/make-connection "photon" :host (:mongodb.host conf)))

(def collection :events)

(def db (ref nil))

(defn m-mongo [conf]
  (if (nil? @db)
    (let [new-db (mongo-conn conf)]
      (dosync (alter db (fn [_] new-db)))
      new-db)
    @db))

(def mongo (memoize m-mongo))

(defn strip-id [payload]
  (dissoc payload :_id))

(defn lazy-events-page [conf stream-name date page]
  (m/with-mongo (mongo conf)
    (let [res (m/fetch collection :where {:stream-name stream-name
                                          :order-id {:$gte date}}
                       :skip (* page-size page) :limit page-size)]
      (log/trace "Calling mongo: " :where {:stream-name stream-name
                                           :order-id {:$gte date}}
                 :skip (* page-size page) :limit page-size)
      (if (< (count res) 1)
        []
        (concat res
                (lazy-seq (lazy-events-page conf stream-name date (inc page))))))))

(defrecord LocalMongoDB [conf]
  db/DB
  (driver-name [this] "mongo")
  (fetch [this stream-name id]
    (let [res (m/with-mongo (mongo conf)
                (m/fetch-one collection :where {:stream-name stream-name
                                                :_id id}))]
      (strip-id res)))
  (delete! [this id]
    (m/with-mongo (mongo conf)
      (m/destroy! collection {:_id id})))
  (delete-all! [this]
    (m/with-mongo (mongo conf)
      (m/destroy! collection {})))
  (search [this id]
    (m/with-mongo (mongo conf)
      (map strip-id (m/fetch :events :where {:_id id}))))
  (distinct-values [this k]
    (m/with-mongo (mongo conf)
      (into #{} (m/distinct-values collection (name k)))))
  (store [this payload]
    (m/with-mongo (mongo conf)
      (m/insert! collection (assoc payload :_id (:order-id payload)))))
  (lazy-events [this stream-name date]
    (let [regex (if (.contains stream-name "**")
                  (re-pattern (clojure.string/replace stream-name #"\*\*" ".*"))
                  stream-name)
          int-date (if (string? date) (read-string date) date)]
      (lazy-events-page conf regex date 0))))

