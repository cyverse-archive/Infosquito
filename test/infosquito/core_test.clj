(ns infosquito.core-test
  (:use clojure.test)
  (:require infosquito.work-queue-test
            infosquito.worker-test))


(run-tests 'infosquito.work-queue-test
           'infosquito.worker-test)
