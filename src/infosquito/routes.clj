(ns infosquito.routes
  "Defines an implementation of the infosquito.handler/ROUTES protocol.  
   Currently, this is just stub code."
  (:use infosquito.elasticsearch
        infosquito.handler)
  (:require [clojure.data.json :as dj]))

(defn- interpret-name
  [name-param]
  (if name-param (.toLowerCase name-param)))

(defn- interpret-sort
  [sort-param]
  (if (nil? sort-param)
    :score-sort
    (condp = (.toLowerCase sort-param)
      "score" :score-sort
      "name"  :name-sort
              nil)))        
  
(defn- interpret-limit-window
  [limit-param]
  (try
    (let [lval (Integer. limit-param)]
      (if (> lval 0) [0 lval]))
    (catch NumberFormatException _)))

(defn- interpret-range-window
  [lb-param ub-param]
  (try
    (let [lb (Integer. lb-param)
          ub (Integer. ub-param)]
      (if (> ub lb) [lb ub]))
    (catch NumberFormatException _)))
      
(defn- interpret-window
  [wnd-param]
  (if (nil? wnd-param)
    [0 10]
    (let [bnds-str (re-seq #"-|[^-]+" wnd-param)]  
      (condp = (count bnds-str)
        1 (interpret-limit-window (first bnds-str))
        3 (interpret-range-window (first bnds-str) (last bnds-str))
          nil))))

(defn- mk-help-msg
  [url]
  (str "Please see " url "."))

(defn- mk-help-resp
  [url response-status]
  {:status  response-status
   :headers {"Content-Type" "text/plain"}
   :body    (mk-help-msg url)})

(defn- mk-welcome-resp
  [help-url]
  {:status  200
   :headers {"Content-Type" "text/plain"}
   :body    (str "Welcome to Infosquito!  " (mk-help-msg help-url))})
  
(defn- mk-bad-param-resp
  []
  {:status  400
   :headers {"Content-Type" "application/json"}
   :body    (dj/json-str {:action     "search"
                          :status     "failure"
                          :error_code "ERR_INVALID_QUERY_STRING"})})

(defn- fake-match
  [glob]
  (if-not (empty? glob)
    (case (first glob)
      \? (str \t (fake-match (next glob)))
      \* (fake-match (next glob))
      \\ (str (fnext glob) (fake-match (nnext glob)))
         (str (first glob) (fake-match (next glob))))))

(defn- mk-valid-search-resp
  [matches]
  (let [response {:action  "search"
                  :status  "success"
                  :matches matches}]
    {:status  200
     :headers {"Content-Type" "application/json"}
     :body    (dj/json-str response)}))
    
(defn mk-routes [es-client help-url]
  "This creates the routes to be used by infosquito.handler/mk-handler function
   as a destination for routed requests.

   PARAMETERS
     es-client - The Elasticsearch client for search cluster
     help-url - The URL that points to the Infosquito help page.

   RETURN
     It returns an anonymous instance of the infosquito.handler/ROUTES."
  (reify ROUTES
    (help [_] 
      (mk-help-resp help-url 200))
  
    (search [_ user name sort window]
      (let [name'   (interpret-name name)
            sort'   (interpret-sort sort)
            window' (interpret-window window)]
        (if (or (nil? user)
                (nil? name')
                (nil? sort')
                (nil? window'))
          (mk-bad-param-resp)
          (mk-valid-search-resp (query es-client 
                                       user 
                                       name' 
                                       sort' 
                                       (window' 0)
                                       (- (window' 1) (window' 0)))))))
    
    (unknown [_]
      (mk-help-resp help-url 404))
    
    (welcome [_] 
      (mk-welcome-resp help-url))))
