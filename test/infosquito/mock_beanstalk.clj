(ns infosquito.mock-beanstalk
  "This is a mock implementation of a Beanstalk Queue.  It does not support
   priority, delay, time to run (TTR), multiple tubes, burying, max-job-size,
   timeouts for reservations, or multiple workers."
  (:require [com.github.drsnyder.beanstalk :as beanstalk]
            [slingshot.slingshot :as ss]))


(def default-state {:closed?   false
                    :oom?      false
                    :draining? false
                    :bury?     false})


(defn- validate-state
  [state]
  (when (:closed? state) (ss/throw+ {:type :connection-closed}))
  (when (:oom? state) (ss/throw+ {:type :protocol :message "OUT_OF_MEMORY"}))
  (when (:draining? state) (ss/throw+ {:type :protocol :message "DRAINING"})))
                                      
  
(defrecord ^{:private true} MockBeanstalk [queue-ref next-id-ref state-ref]
  beanstalk/BeanstalkObject
  
  (close [_]
    (locking _
      (swap! state-ref #(assoc % :closed? true))
      nil))
  
  (put [_ pri delay ttr bytes data]
    (locking _
      (validate-state @state-ref)
      (let [id  @next-id-ref]
        (swap! next-id-ref inc)
        (when (:bury? @state-ref) 
          (ss/throw+ {:type :protocol :message (str "BURIED " id)}))
        (swap! queue-ref #(conj % {:id id :payload data}))
        (.notify _)
        (str "INSERTED " id beanstalk/crlf))))
       
  (reserve [_]
    (locking _ 
      (validate-state @state-ref)
      (if (empty? @queue-ref) (.wait _))
      (first @queue-ref)))
                
  (reserve-with-timeout [_ timeout]
    (.reserve _))
       
  (delete [_ id]
    (locking _
      (validate-state @state-ref)
      (let [[pre [subj & post]] (split-with #(not= id (:id %)) @queue-ref)]
        (if (nil? subj) 
          (str "NOT_FOUND" beanstalk/crlf)
          (do
            (reset! queue-ref (concat pre post))
            (str "DELETED" beanstalk/crlf)))))))


(defn mk-mock-beanstalk
  [queue-ref & [state-ref]]
  (when (and state-ref (:closed? @state-ref)) (ss/throw+ (Exception.)))
  (let [next-id (if (empty? @queue-ref)
                  0
                  (inc (apply max (map :id @queue-ref))))]
    (->MockBeanstalk queue-ref 
                     (atom next-id) 
                     (if state-ref state-ref (atom default-state)))))
