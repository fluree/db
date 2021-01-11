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
      (let [auth-ident    (if (string? auth_id)
                            ["_auth/id" auth_id]            ;; if a string, assume it is an improperly formed identity for _auth/id
                            auth_id)
            auth-roles-ch (<? (dbproto/-query db {:select "?roles"
                                                  :where  [[auth-ident "_auth/roles" "?roles"]]}))]
        (or (not-empty auth-roles-ch)
            (<? (dbproto/-query db {:select "?roles"        ;; user roles, if exist, act as defaults if no auth roles
                                    :where  [["?user" "_user/auth" auth-ident]
                                             ["?user" "_user/roles" "?roles"]]})))))))


(defn root-role?
  "Given an _auth identity, returns roles directly associated
  with the auth ident.  If none found, attempts to retrieve
  default roles for an associated user."
  [db auth_id]
  (go-try
    ;; Lookup auth roles
    (if (nil? auth_id)
      false
      (let [auth-ident    (if (string? auth_id)
                            ["_auth/id" auth_id]            ;; if a string, assume it is an improperly formed identity for _auth/id
                            auth_id)
            auth-roles-ch (<? (dbproto/-query db {:select "?role"
                                                  :where  [["?role", "_role/id", "root"]
                                                           [auth-ident "_auth/roles" "?role"]]
                                                  :opts   {:cache true}}))]
        (or (not-empty auth-roles-ch)
            (not-empty (<? (dbproto/-query db {:select "?role"
                                               :where  [["?role", "_role/id", "root"]
                                                        ["?user" "_user/auth" auth-ident]
                                                        ["?user" "_user/roles" "?roles"]]
                                               :opts   {:cache true}}))))))))

