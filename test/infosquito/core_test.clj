(ns infosquito.core-test
  (:use clojure.test)
  (:require infosquito.props-test 
            infosquito.worker-test))


(run-tests 'infosquito.props-test
           'infosquito.worker-test)
