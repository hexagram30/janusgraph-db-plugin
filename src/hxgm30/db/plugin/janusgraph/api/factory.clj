(ns hxgm30.db.plugin.janusgraph.api.factory
  (:require
    [clojure.java.io :as io]
    [taoensso.timbre :as log])
  (:import
    (java.nio.file Paths)
    (java.net URI)
    (org.janusgraph.core ConfiguredGraphFactory JanusGraph JanusGraphFactory))
  (:refer-clojure :exclude [drop]))

(load "/hxgm30/graphdb/plugin/protocols/factory")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;   Utility/Support Functions   ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- create-data-dir!
  [file]
  (when-not (.exists file)
    (->> file
         (.getPath)
         (format "%s/nil")
         (io/make-parents))))

(defn- get-fs-path
  [protocol file]
  (->> file
       (.getAbsolutePath)
       (format "%s:%s" protocol)
       (new URI)
       (Paths/get)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;   API Implementation   ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- -close
  [this ^JanusGraph db]
  (JanusGraphFactory/drop db))

(defn- -connect
  ([this]
    (-connect this {}))
  ([this opts]
    (let [builder (JanusGraphFactory/build)]
      (log/trace "Builder:" builder)
      (log/debug "Configuring builder with:" opts)
      ;(.set builder "gremlin.graph" "org.janusgraph.core.JanusGraphFactory")
      ;(.set builder "graph.graphname" "game")
      (.set builder "storage.backend" (:storage-backend opts))
      (.set builder "storage.directory" (:storage-directory opts))
      (.open builder))))

(defn- -dbs
  [this]
  (JanusGraphFactory/getGraphNames))

(defn- -drop
  [this ^JanusGraph db]
  (JanusGraphFactory/drop db))

(defn- -destroy
  [this]
  ;; No-op
  )

(def behaviour
  {:connect -connect
   :dbs -dbs
   :destroy -destroy
   :drop -drop})

(extend JanusGraphFactory
        DBFactoryAPI
        behaviour)

(defn create
  []
  (new JanusGraphFactory))
