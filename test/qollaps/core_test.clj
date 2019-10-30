(ns qollaps.core-test
  (:require [qollaps.core :as sut]
            [clojure.spec.alpha :as s]
            [cognitect.aws.qldb-session.specs :as qldb-session-spec]
            [cognitect.aws.qldb.specs :as qldb-spec]
            [clojure.test :as t]
            [cognitect.aws.client.api :as aws]))

(defn test-fixture [f]
  (reset! sut/clients {:qldb {} :qldb-session {}})
  (f)
  (reset! sut/clients {:qldb nil :qldb-session nil}))

(t/use-fixtures :once test-fixture)

(t/deftest qldb
  (t/testing "create ledger"
    (with-redefs [sut/handle-result identity
                  aws/invoke (fn [_ request]
                               (t/is (= :CreateLedger (:op request)))
                               (t/is (s/valid? :cognitect.aws.qldb/CreateLedgerRequest (:request request))))]
      (sut/create-ledger "ledger-name" {} false))))
