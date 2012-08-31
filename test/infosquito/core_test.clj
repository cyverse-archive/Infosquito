(ns infosquito.core-test
  (:use clojure.test)
  (:require infosquito.handler-test
            infosquito.routes-test
            infosquito.search-test))

(run-tests 'infosquito.handler-test
           'infosquito.routes-test
           'infosquito.search-test)
