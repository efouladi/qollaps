(ns qollaps
  (:require [clojure.data.json :as json]
            [cognitect.aws.client.api :as aws]
            [cognitect.aws.credentials :as cred])
  (:import [com.amazon.ion.system IonBinaryWriterBuilder IonReaderBuilder IonSystemBuilder IonTextWriterBuilder]
           software.amazon.qldb.QldbHash
           java.io.ByteArrayOutputStream))

(def qldb-session (aws/client {:api :qldb-session :region "eu-west-1"}))

(def ion-system-builder (.build (IonSystemBuilder/standard)))

(defn clj->ion
  [clj-value]
  (let [value (->> clj-value
                   json/write-str
                   (.singleValue ion-system-builder))]
    (.makeReadOnly value)
    value))

(defn clj->ion-binary
  [clj-value]
  (with-open [os (ByteArrayOutputStream.)
              writer (.build (IonBinaryWriterBuilder/standard) os)
              reader (.build (IonReaderBuilder/standard) (json/write-str clj-value))]
    (.writeValues writer reader)
    os))

(defn ion->json
  [ion-value]
  (let [sb (StringBuilder.)
        writer (.build (.withJsonDowngrade (IonTextWriterBuilder/json)) sb)
        reader (.build (IonReaderBuilder/standard) ion-value)]
    (.writeValues writer reader)
    (str sb)))

(defn get-qldb-hash-obj [arg]
  (QldbHash/toQldbHash arg ion-system-builder))

(defn- compute-hash [& args]
  (reduce #(.dot %1 (get-qldb-hash-obj %2))
          (get-qldb-hash-obj (first args))
          (rest args)))

(defn- get-transaction-hash [tx-id & statements]
  (.getQldbHash (reduce #(.dot  %1 (apply compute-hash %2))
                        (compute-hash tx-id)
                        statements)))


(defn start-session [ledger-name]
  (let [result (aws/invoke qldb-session {:op :SendCommand
                                         :request {:StartSession {:LedgerName ledger-name}}})]
    (get-in result [:StartSession :SessionToken] result)))

(defn start-transaction [session-token]
  (let [result (aws/invoke qldb-session {:op :SendCommand
                                         :request {:SessionToken session-token
                                                   :StartTransaction {}}})]
    (get-in result [:StartTransaction :TransactionId] result)))

(defn execute-statement
  [session-token tx-id statement params]
  (aws/invoke qldb-session {:op :SendCommand
                            :request {:SessionToken session-token
                                      :ExecuteStatement {:TransactionId tx-id
                                                         :Statement statement
                                                         :Parameters (mapv #(hash-map :IonBinary (-> %
                                                                                                     clj->ion-binary
                                                                                                     .toByteArray)) params)}}}))

(defn execute-statements [session-token tx-id statements]
  (doall (map (fn [[statement & params]]
                (execute-statement session-token tx-id statement params)) statements)))

(defn commit-transaction [session-token transaction-id query]
  (aws/invoke qldb-session {:op :SendCommand
                            :request {:SessionToken session-token
                                      :CommitTransaction {:TransactionId transaction-id
                                                          :CommitDigest (apply get-transaction-hash transaction-id query)}}}))

(defn abort-transaction [session-token]
  (aws/invoke qldb-session {:op :SendCommand
                            :request {:SessionToken session-token
                                      :AbortTransaction {}}}))

(defn end-session [session-token]
  (aws/invoke qldb-session {:op :SendCommand
                            :request {:SessionToken session-token
                                      :EndSession {}}}))
