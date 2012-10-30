(ns infosquito.es
  (:use infosquito.es-if)
  (:require [clojurewerkz.elastisch.rest :as cer]
            [clojurewerkz.elastisch.rest.document :as cerd]
            [clojurewerkz.elastisch.rest.index :as ceri]
            [cheshire.custom :as json]
            [clj-http.client :as http]))


;; FUNCTIONS THAT SHOULD BE IN clojurewerkz.elastisch.rest


(defn- cer_post-text
  [^String uri &{:keys [body] :as options}]
  (io! (json/decode (:body (http/post uri 
                                      (merge options {:accept :json}))) 
                    true)))
        

(defn- cer_scroll-url
  []
  (cer/url-with-path "_search" "scroll"))


(defrecord ^{:private true} Indexer []
  Indexes
  
  (delete [_ index type id]
    (cerd/delete index type id))
  
  (exists? [_ index]
    (ceri/exists? index))
  
  (put [_ index type id doc-map]
    (cerd/put index type id doc-map))
  
  (scroll [_ scroll-id keep-alive-time]
    (cer_post-text (cer_scroll-url)
                   :query-params {:scroll keep-alive-time} 
                   :body         scroll-id))

  (search-all-types [_ index params]
    (apply cerd/search-all-types index (flatten (vec params)))))
  

(defn mk-indexer
  [es-url]
  (cer/connect! es-url)
  (->Indexer))