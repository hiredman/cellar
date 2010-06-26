;; TODO:
;; Checksums
;; Versioning
;; Deletion

(ns cellar.s3
  (:require [clojure-http.resourcefully :as res]
            [cellar.core]
            [clojure.xml])
  (:import [javax.crypto Mac]
           [javax.crypto.spec SecretKeySpec]
           [sun.misc BASE64Encoder]
           [java.util Date]
           [java.text SimpleDateFormat FieldPosition]
           [java.security MessageDigest DigestInputStream]))

(def rest-end-point "https://s3.amazonaws.com")

(defrecord S3 [aws-key aws-secret-key])

(defrecord S3Box [container name thing service]
  Object
  (toString [this] (format "%s/%s" container name)))

(def date-formater (SimpleDateFormat. "EEE, d MMM yyyy HH:mm:ss Z"))

(defn date []
  (let [date (Date.)
        sb (StringBuffer.)]
    (.toString (.format date-formater date sb (FieldPosition. 0)))))

(defn string-to-sign [verb md5 type date uri]
  (format "%s\n%s\n%s\n%s\n%s"
          verb
          md5
          type
          date
          uri))

(defn sign [service string-to-sign]
  (let [key (SecretKeySpec. (.getBytes (:aws-secret-key service)) "HmacSHA1")
        mac (Mac/getInstance "HmacSHA1")]
    (.init mac key)
    (.encode (BASE64Encoder.) (.doFinal mac (.getBytes string-to-sign)))))

(defmacro amazon [service [method uri headers & rst]]
  `(let [service# ~service
         date# (date)
         meth# ~method
         [uri# query#] (.split ~uri "\\?")
         thing# (format "%s/%s" rest-end-point uri#)
         thing# (if query# (str thing# "?" query#) thing#)
         sts# (string-to-sign
               (.toUpperCase (name '~method))
               ""
               ""
               date#
               (str "/" uri#))
         headers# (assoc ~headers
                    "Authorization" (format "AWS %s:%s"
                                            (:aws-key service#)
                                            (sign
                                             service#
                                             sts#))
                    "Date" date#)]
     (meth# thing# headers# ~@rst)))

(defn ls-fn [service container prefix]
  (letfn [(list
           [service container prefix marker]
           (lazy-seq
            (let [results (list′ service container prefix marker)]
              (concat (seq results)
                      (when @results
                        (list service container prefix @results))))))
          (list′
           [service container prefix marker]
           (with-open [result (->> (amazon
                                    service
                                    (res/get
                                     (if prefix
                                       (format
                                        "%s?prefix=%s&marker=%s&max-keys=500"
                                        container prefix marker)
                                       (format "%s?marker=%s&max-keys=500"
                                               container marker))))
                                   :connection
                                   .getInputStream)]
             (let [xml (clojure.xml/parse result)]
               (reify
                clojure.lang.Seqable
                (seq
                 [this]
                 (->>  xml
                       (tree-seq map? :content)
                       (filter #(= :Contents (:tag %)))
                       (map
                        (fn [x]
                          (S3Box.
                           container
                           (extract-keys x)
                           x)))))
                clojure.lang.IDeref
                (deref
                 [this]
                 (:name (last this)))))))
          (extract-keys
           [x]
           (->> x
                :content
                (filter #(= :Key (:tag %)))
                first
                :content
                first))]
    (list service container prefix "")))

(extend-protocol
 cellar.core/Cellar
 S3
 (get
  [service container name]
  (.getOutputStream
   (:connection
    (amazon
     service
     (res/get (format "%s/%s" container name))))))
 (list-containers
  [service]
  (let [date (date)]
    (with-open [is (.getInputStream
                    (:connection
                     (amazon service (res/get ""))))]
      (->> is clojure.xml/parse
           :content
           (filter #(= :Buckets (:tag %)))
           first
           :content
           (mapcat :content)
           (filter #(= :Name (:tag %)))
           (map (comp first :content))))))
 (ls [service container prefix]
     (ls-fn service container prefix))
 (put [service container name data]
      (amazon
       service
       (res/put
        (format "%s/%s" container name)
        {}
        data)))
 (exists? [service container name]
          (try
           (cellar.core/get service container name)
           true
           (catch Exception _
             false)))
 (create-container
  [service container]
  (:code (amazon service (res/put container))))
 (delete-container
  [service container]
  (amazon service (res/delete container)))
 (delete
  [service container name]
  (amazon service (res/delete (format "%s/%s" container name)))))

(extend-protocol
 cellar.core/Box
 (content
  [box]
  (cellar.core/get (:service box) (:container box) (:name box)))
 (meta
  [box]
  (:thing box)))
