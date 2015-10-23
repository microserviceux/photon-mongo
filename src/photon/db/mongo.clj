(ns photon.db.mongo
  (:require [photon.db :as db]
            [photon.config :as conf]
            [clojure.tools.logging :as log]
            [somnium.congomongo :as m]))

(def page-size 100)

(defn mongo-conn []
  (m/make-connection "photon" :host (:mongodb.host conf/config)))

(def collection :events)

(def db (ref nil))

(defn m-mongo []
  (if (nil? @db)
    (let [new-db (mongo-conn)]
      (dosync (alter db (fn [_] new-db)))
      new-db)
    @db))

(def mongo (memoize m-mongo))

(db/defdbplugin LocalMongoDB []
  db/DB
  (driver-name [this] "mongo")
  (fetch [this stream-name id]
    (m/with-mongo (mongo)
      (m/fetch-one collection :where {:stream-name stream-name
                                      :_id id})))
  (delete! [this id]
    (m/with-mongo (mongo)
      (m/destroy! collection {:_id id})))
  (delete-all! [this]
    (m/with-mongo (mongo)
      (m/destroy! collection {})))
  (put [this data]
    (m/with-mongo (mongo)
      (m/insert! collection data)))
  (search [this id] (db/fetch this {:$exists true} id))
  (distinct-values [this k]
    (m/with-mongo (mongo)
      (m/distinct-values collection k)))
  (store [this payload]
    (m/with-mongo (mongo)
      (m/insert! collection (merge payload {:_id (:local-id payload)}))))
  (lazy-events [this stream-name date]
    (db/lazy-events-page this stream-name date 0)) 
  (lazy-events-page [this stream-name date page]
    (m/with-mongo (mongo)
      (let [l-date (if (string? date) (read-string date) date)
            res (m/fetch collection :where {:stream-name stream-name}
                         :skip (* page-size page) :limit page-size)]
        (log/info "Calling mongo: " :where {:stream-name stream-name}
                  :skip (* page-size page) :limit page-size)
        (if (< (count res) 1)
          []
          (concat res
                  (lazy-seq (db/lazy-events-page this stream-name l-date (inc page)))))))))

