(ns infosquito.core-test
  (:use clojure.test)
  (:require infosquito.elasticsearch-test
            infosquito.handler-test
            infosquito.routes-test))

(run-tests 'infosquito.elasticsearch-test
           'infosquito.handler-test
           'infosquito.routes-test)
