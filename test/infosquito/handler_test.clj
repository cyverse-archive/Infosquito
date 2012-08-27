(ns infosquito.handler-test
  (:use clojure.test
        infosquito.handler)
  (:require [ring.mock.request :as rmr]))

(defn- resp-for-req
  [& args]
  (let [handler (mk-handler 
                  (reify ROUTES
                    (help [_] "help")
                    (search [_ user name sort window] 
                      (print-str "search" user name sort window))
                    (unknown [_] "unknown")                    
                    (welcome [_] "welcome")))]
    (handler (apply rmr/request args))))
    
(defn- strip-body
  [resp]
  (assoc resp :body nil))

(deftest test-welcome
  (let [get-resp (resp-for-req :get "/")]
    (is (= "welcome" (:body get-resp)))
    (is (= (resp-for-req :head "/") (strip-body get-resp)))))

(deftest test-help
  (letfn [(is-help [path] (is "help" (:body (resp-for-req :options path))))]
    (is-help "/")
    (is-help "/search")
    (is-help "/path")))
  
(deftest test-search
  (let [params        {:n "n" :u "u" :sort "score" :window "10"}
        good-get-resp (resp-for-req :get "/search" params)]
    (is (= "search u n score 10" (:body good-get-resp)))
    (is (= (resp-for-req :head "/search" params) (strip-body good-get-resp))))) 

(deftest test-unknown
  (let [is-unknown (fn [& args] (is (= "unknown" 
                                       (:body (apply resp-for-req args)))))
        ns-path    "/not-search"
        sc-path    "/search/child"]
    (is-unknown :get ns-path)
    (is (= (resp-for-req :head ns-path) (strip-body (resp-for-req :get ns-path))))
    (is-unknown :get sc-path)
    (is (= (resp-for-req :head sc-path) (strip-body (resp-for-req :get sc-path))))
    (is-unknown :post "/search")
    (is-unknown :put "/search")
    (is-unknown :delete "/search")
    (is-unknown :trace "/search")
    (is-unknown :connect "/search")
    (is-unknown :patch "/search")))
