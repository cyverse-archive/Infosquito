(ns infosquito.routes-test
  (:use clojure.test
        infosquito.es-fixture
        infosquito.routes
        infosquito.handler)
  (:require [clojure.data.json :as dj]))

(def ^{:private true} es-client (atom nil))

(use-fixtures :once (mk-local-cluster es-client))

(defn- is-response-helpful 
  [route status]
  (let [url  "http://url"
        resp (route (mk-routes @es-client url))]
    (is (= (:status resp) status))
    (is (= (:headers resp) {"Content-Type" "text/plain"}))
    (is (not (nil? (re-find (re-pattern url) (:body resp)))))))
  
(defn- chk-search-response
  [req-params resp-status chk-body]
  (let [resp (search (mk-routes @es-client "url") 
                     (:user req-params)
                     (:name req-params)
                     (:sort req-params)
                     (:window req-params))
        body (:body resp)]
    (is (= (:status resp) resp-status))
    (is (= (:headers resp) {"Content-Type" "application/json"}))
    (is (not (nil? body)))
    (if body
      (chk-body (dj/read-json body true)))))

(defn- is-search-bad
  [req-params]
  (chk-search-response req-params 
                       400 
                       #(is (= % {:action     "search"
                                  :status     "failure"
                                  :error_code "ERR_INVALID_QUERY_STRING"}))))

(defn- is-window-bad
  [window-param]
  (is-search-bad {:user   "user"
                  :name   "name"
                  :window window-param}))

(deftest test-help-response
  (is-response-helpful help 200))

(deftest test-search-response-default-params
  (chk-search-response {:user "user1" :name "file"} 
                       200 
                       #(is (= % 
                               {:action  "search"
                                :status  "success"
                                :matches [{:index 0
                                           :path  "/iplant/home/user1/file"
                                           :name  "file"}]}))))
(deftest test-search-response-no-user
  (is-search-bad {:name "name"}))

(deftest test-search-response-no-name
  (is-search-bad {:user "user"})) 

(deftest test-search-response-with-sort
  (testing "sort by score is same as default"
    (let [routes (mk-routes @es-client "url")]
      (is (= (search routes "user" "name" "score" nil) 
             (search routes "user" "name" nil nil)))))
  (testing "sort by name"
    (chk-search-response {:user "user1" 
                          :name "file" 
                          :sort "name"} 
                         200 
                         #(is (= % 
                                 {:action  "search"
                                  :status  "success"
                                  :matches [{:index 0
                                             :path  "/iplant/home/user1/file"
                                             :name  "file"}]}))))
  (testing "bad sort value"
    (is-search-bad {:user "user"
                    :name "name"
                    :sort ""})
    (is-search-bad {:user "user"
                    :name "name"
                    :sort "bad"})))

(deftest test-search-response-with-window
  (testing "[0-10) is default window"
    (let [routes      (mk-routes @es-client "url")
          default-res (search routes "user" "name" nil nil)]
      (is (= default-res (search routes "user" "name" nil "10")))
      (is (= default-res (search routes "user" "name" nil "0-10")))))
  (testing "windowing with limit"
    (chk-search-response {:user   "user1" 
                          :name   "file" 
                          :window "1"} 
                         200 
                         #(is (= % 
                                 {:action  "search"
                                  :status  "success"
                                  :matches [{:index 0
                                             :path  "/iplant/home/user1/file"
                                             :name  "file"}]}))))  
  (testing "windowing with range"
    (chk-search-response {:user   "user2" 
                          :name   "*"
                          :sort   "name"
                          :window "1-2"} 
                         200 
                         #(is (= % 
                                 {:action  "search"
                                  :status  "success"
                                  :matches [{:index 1
                                             :path  "/iplant/home/user2/file"
                                             :name  "file"}]}))))  
  (testing "bad window values"
    (is-window-bad "")
    (is-window-bad "-")
    (is-window-bad "-1")
    (is-window-bad "1-")
    (is-window-bad "1--20")
    (is-window-bad "1-20-300")
    (is-window-bad "1.2")
    (is-window-bad "2-2")
    (is-window-bad "1.2-4")
    (is-window-bad "1-4.2")))  
    
(deftest test-unknown-response
  (is-response-helpful unknown 404))
  
(deftest test-welcome-response
  (is-response-helpful welcome 200))
