(ns qollaps.core
  (:require [clojure.data.json :as json]
            [cognitect.aws.client.api :as aws]
            [dandelion.core :refer [clj->ion-binary ion->json clj->ion]])
  (:import com.amazon.ion.system.IonSystemBuilder
           software.amazon.qldb.QldbHash))

(def clients (atom {:qldb nil
                    :qldb-session nil}))

(defn get-client! [client]
  (if-let [c (get @clients client)]
    c
    (get (swap! clients assoc client (aws/client {:api client})) client)))

(defn- get-qldb-hash-obj [arg]
  (QldbHash/toQldbHash (clj->ion arg) (.build (IonSystemBuilder/standard))))

(defn- compute-hash [& args]
  (reduce #(.dot %1 (get-qldb-hash-obj %2))
          (get-qldb-hash-obj (first args))
          (rest args)))

(defn- handle-result [result]
  (if (not= (some-> result meta :http-response :status (quot 100)) 2)
    (throw (ex-info "Operation failed" result))
    result))

(defn ion-binaries->clj-data [values]
  (map #(-> %
            :IonBinary
            ion->json
            json/read-str) values))

(defn get-transaction-hash [tx-id & statements]
  (.getQldbHash (reduce #(.dot  %1 (apply compute-hash %2))
                        (compute-hash tx-id)
                        statements)))

(defn create-ledger [ledger-name tags deletion-protection]
  (handle-result
   (aws/invoke (get-client! :qldb) {:op :CreateLedger
                     :request {:Name ledger-name
                               :Tags tags
                               :PermissionsMode "ALLOW_ALL"
                               :DeletionProtection deletion-protection}})))

(defn describe-ledger [ledger-name]
  (handle-result
   (aws/invoke (get-client! :qldb) {:op :DescribeLedger
                     :request {:Name ledger-name}})))

(defn start-session [ledger-name]
  (let [result (aws/invoke (get-client! :qldb-session) {:op :SendCommand
                                         :request {:StartSession {:LedgerName ledger-name}}})]

    (-> result
        handle-result
        (get-in [:StartSession :SessionToken] result))))

(defn start-transaction [session-token]
  (let [result (aws/invoke (get-client! :qldb-session) {:op :SendCommand
                                         :request {:SessionToken session-token
                                                   :StartTransaction {}}})]
    (-> result
        handle-result
        (get-in [:StartTransaction :TransactionId] result))))

(defn execute-statement
  [session-token tx-id statement params]
  (let [result (aws/invoke (get-client! :qldb-session) {:op :SendCommand
                                         :request {:SessionToken session-token
                                                   :ExecuteStatement {:TransactionId tx-id
                                                                      :Statement statement
                                                                      :Parameters (mapv #(hash-map :IonBinary (clj->ion-binary %)) params)}}})]
    (-> result
        handle-result
        :ExecuteStatement
        (update-in [:FirstPage :Values] ion-binaries->clj-data))))

(defn execute-statements [session-token tx-id statements]
  (doall (map (fn [[statement & params]]
                (execute-statement session-token tx-id statement params)) statements)))

(defn commit-transaction [session-token transaction-id statements]
  (handle-result (aws/invoke (get-client! :qldb-session) {:op :SendCommand
                                           :request {:SessionToken session-token
                                                     :CommitTransaction {:TransactionId transaction-id
                                                                         :CommitDigest (apply get-transaction-hash transaction-id statements)}}})))

(defn abort-transaction [session-token]
  (handle-result (aws/invoke (get-client! :qldb-session) {:op :SendCommand
                                           :request {:SessionToken session-token
                                                     :AbortTransaction {}}})))

(defn end-session [session-token]
  (handle-result (aws/invoke (get-client! :qldb-session) {:op :SendCommand
                                           :request {:SessionToken session-token
                                                     :EndSession {}}})))

(defn query [ledger-name & statements]
  "Takes the ledger name and any number of statements and run them
  in a transaction.
  Each statement is a vector of strings with first element being the query
  string and optional extra elements each being a parameter for the query"
  (let [session-token (start-session ledger-name)]
    (try
      (let [tx-id (start-transaction session-token)
            results (execute-statements session-token tx-id statements)]
        (commit-transaction session-token tx-id statements)
        results)
      (catch Exception e (do (abort-transaction session-token) (throw e)))
      (finally (end-session session-token)))))
