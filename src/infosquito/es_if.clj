(ns infosquito.es-if)

(defprotocol Indexes
  
  (delete [_ index type id]
    "Maps to clojurewerkz.elastisch.rest.document/delete")
  
  (exists? [_ index]
    "Maps to clojurewerkz.elastisch.rest.index/exists?")
    
  (put [_ index type id doc-map]
    "Maps to clojurewerkz.elastisch.rest.document/put")
  
  (search-all-types [_ index query]
    "Maps to clojurewerkz.elastisch.rest.document/search-all-types"))