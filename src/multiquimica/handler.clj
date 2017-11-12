(ns multiquimica.handler
  (:require [compojure.core :refer :all]
            [compojure.route :as route]
            [cheshire.core :as json]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [ring.middleware.defaults :refer [wrap-defaults site-defaults]]))

(defn json-seq [filename]
  (with-open [reader (io/reader (io/resource filename))]
    (doall
      (first (json/parsed-seq reader true)))))

(defn csv-seq [filename]
  (with-open [reader (io/reader (io/resource filename))]
    (doall
      (csv/read-csv reader))))

(defn query-writer [json csv]
  (let [num (:first-line json)
        name (:table-name json)
        directives (:directives json)]
  
  (let [init  (reduce #(str %1 (get %2 :column-name) 
                        (if (= (get %2 :column-name) (:column-name (last directives))) ") " ", "))
              (str "INSERT INTO " name " (") directives)
        clean-csv (drop num csv)]
        
        (map (fn [csv-line]
              (reduce #(str %1 (nth csv-line (get %2 :column-index))
                        (if (= (get %2 :column-index) (:column-index (last directives))) "); " ", "))
              (str init "VALUES (")  directives)) 
        clean-csv))))

(defroutes app-routes
  (GET "/" [] "Hello World")
  (GET "/processData" [metadata-fs data-fs]
    (query-writer 
      (json-seq (str "public/" metadata-fs ".json"))
      (csv-seq (str "public/" data-fs ".csv"))))
  (route/not-found "Not Found"))

(def app
  (wrap-defaults app-routes site-defaults))