(ns infosquito.mock-beanstalk
  "This is a mock implementation of a Beanstalk Queue.  It does not support
   priority, delay, time to run (TTR), multiple tubes, burying, max-job-size,
   timeouts for reservations, or multiple workers."
  (:require [com.github.drsnyder.beanstalk :as cgdb]))


(defrecord ^{:private true} MockBeanstalk [queue-ref next-id-ref]
  cgdb/BeanstalkObject
  
  (put [_ pri delay ttr bytes data]
    (locking _
      (let [id  @next-id-ref]
        (swap! next-id-ref inc)
        (swap! queue-ref #(conj % {:id id :payload data}))
        (.notify _)
        (str "INSERTED " id cgdb/crlf))))
       
  (reserve [_]
    (locking _ 
      (if (empty? @queue-ref) (.wait _))
      (first @queue-ref)))
                
  (reserve-with-timeout [_ timeout]
    (.reserve _))
       
  (delete [_ id]
    (locking _
      (let [[pre [subj & post]] (split-with #(not= id (:id %)) @queue-ref)]
        (if (nil? subj) 
          (str "NOT_FOUND" cgdb/crlf)
          (do
            (reset! queue-ref (concat pre post))
            (str "DELETED" cgdb/crlf)))))))


(defn mk-mock-beanstalk
  [queue-ref]
  (let [next-id (if (empty? @queue-ref)
                  0
                  (inc (apply max (map :id @queue-ref))))]
    (->MockBeanstalk queue-ref (atom next-id))))
