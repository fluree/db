(ns fluree.db.class.prefix)

;; handling prefixes:
;; _prefix/name _prefix/uri
;; i.e: "rdf"  "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
;; have a prefix/name of "" (empty string), which stores a default URI
;;
;; compatibility:
;; current predicates are namespaced like: aaa/bbb - they should continue to work fine
;; -- allow non-namespaced predicates
;; -- create a _default prefix URI of http://data.flur.ee/ for new instances at bootstrap.
;;    this will allow us to provide sample data using that URI and everything will work without namespacing
;;

(defn from-name
  "Returns two-tuple of [prefix name] from an IRI capable string (i.e. _predicate/name).
  Note a prefix can be an empty string, i.e. ':mypred' will return ['' 'mypred],
  whereas 'mypred' would return [nil 'mypred'], and 'my:pred' would return ['my' 'pred']"
  [pred-name]
  (if-let [[_ prefix rest] (re-find #"^([^:]*):(.+$)" pred-name)]
    [prefix rest]
    [nil pred-name]))

(comment


  (predicate-prefix ":me")

  )