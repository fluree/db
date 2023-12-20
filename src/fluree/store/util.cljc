(ns fluree.store.util)

(defn hashable?
  [x]
  (or (string? x)
      (bytes? x)))
