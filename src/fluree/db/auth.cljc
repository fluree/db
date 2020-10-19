(ns fluree.db.auth
  (:require [fluree.db.dbproto :as dbproto]
            [fluree.db.util.async :refer [<? go-try]]))


(defn roles
  "Given an _auth identity, returns associated roles.
  First returns roles directly associated with auth ident.
  If none exist, attempts to retrieve default roles for associated
  user, if they exist."
  [db auth_id]
  (go-try
    (if (= 0 auth_id)                                       ;; root user, special case
      []
      ;; Lookup both auth roles and user roles in parallel
      (let [auth-roles-ch (dbproto/-query db {:select "?roles"
                                              :where  [[auth_id "_auth/roles" "?roles"]]})
            user-roles-ch (dbproto/-query db {:select "?roles"
                                              :where  [["?user" "_user/auth" auth_id]
                                                       ["?user" "_user/roles" "?roles"]]})]
        (if-let [auth-roles (not-empty (<? auth-roles-ch))]
          auth-roles
          (<? user-roles-ch))))))


(defn root-role?
  "Given an _auth identity, returns roles directly associated
  with the auth ident.  If none found, attempts to retrieve
  default roles for an associated user."
  [db auth_id]
  (go-try
    ;; Lookup auth roles
    (if (nil? auth_id)
      false
      (let [auth-roles-ch (if (number? auth_id)
                            (dbproto/-query db {:select "?role"
                                                :where  [["?role", "_role/id", "root"]
                                                         [auth_id "_auth/roles" "?role"]]})
                            (dbproto/-query db {:select "?role"
                                                :where  [["?role", "_role/id", "root"]
                                                         [["_auth/id" auth_id] "_auth/roles" "?role"]]}))
            user-roles-ch (if (number? auth_id)
                            (dbproto/-query db {:select "?role"
                                                :where  [["?role", "_role/id", "root"]
                                                         ["?user" "_user/auth" auth_id]
                                                         ["?user" "_user/roles" "?roles"]]})
                            (dbproto/-query db {:select "?role"
                                                :where  [["?role", "_role/id", "root"]
                                                         ["?user" "_user/auth" ["_auth/id" auth_id]]
                                                         ["?user" "_user/roles" "?roles"]]}))]
        (or (not-empty (<? auth-roles-ch))
            (not-empty (<? user-roles-ch)))))))

