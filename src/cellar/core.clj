(ns cellar.core
  (:refer-clojure :exclude [get list meta]))

(defprotocol Cellar
  (get "gets the named thing from the given container" [service container name])
  (list-containers "lists containers from service" [service])
  (ls "list things that start with the given prefix" [service container prefix])
  (put "put thing in container with given name" [service container name data])
  (exists? "does something with this name exist" [service container name])
  (create-container "create a container" [service container])
  (delete-container "delete a container" [service container])
  (delete "delete a key" [service container name]))

(defprotocol Box
  (content [box])
  (meta [box]))

(defprotocol VersionedCellar
  (create-versioned-container [service container])
  (get-version [service container name version]))

(defprotocol VersionedBox
  (versions [box]))
