(ns fluree.db.indexer.hll
  "HyperLogLog cardinality estimation with precision p=8 (256 registers).

  Based on HyperLogLog++ algorithm:
  - Precision p=8 gives m=256 registers
  - Expected relative standard error ~6.5%
  - Register size: 6 bits (max value 63)
  - Memory per sketch: 256 bytes

  Reference: Flajolet et al., 'HyperLogLog: the analysis of a near-optimal
  cardinality estimation algorithm' (2007)"
  #?(:clj  (:import [java.security MessageDigest]
                    [java.nio ByteBuffer])
     :cljs (:require [goog.crypt :as crypt]
                     [goog.crypt.Sha256]
                     [goog.crypt.base64 :as base64])))

;; HLL Configuration
(def ^:const precision 8)
(def ^:const num-registers (bit-shift-left 1 precision)) ; 2^8 = 256
(def ^:const register-mask 0x3F) ; 63 in binary: 111111 (6 bits)

;; Alpha constant for bias correction (depends on m=256)
(def ^:const alpha-256 0.7213475204444817)

;; Small/large range correction thresholds for m=256
(def ^:const small-range-threshold (* 2.5 num-registers)) ; ~640
(def ^:const large-range-threshold 143165576.53) ; 2^32 / 30 (literal for CLJS compat)

;; Hash functions
(defn- hash-bytes
  "Hash a byte array to 32-bit unsigned integer using SHA-256 (take first 4 bytes).

  Uses 32-bit hash for consistency across CLJ/CLJS and to avoid bit-shift overflow in CLJS."
  [bytes]
  #?(:clj
     (let [digest (MessageDigest/getInstance "SHA-256")
           hashed (.digest digest bytes)
           buffer (ByteBuffer/wrap hashed)]
       ;; Read first 4 bytes as int, convert to unsigned (positive) long
       (bit-and (.getInt buffer) 0xFFFFFFFF))
     :cljs
     (let [sha256 (goog.crypt.Sha256.)
           _ (.update sha256 bytes)
           hashed (.digest sha256)
           ;; Take first 4 bytes and convert to unsigned 32-bit int
           b0 (bit-and (aget hashed 0) 0xFF)
           b1 (bit-and (aget hashed 1) 0xFF)
           b2 (bit-and (aget hashed 2) 0xFF)
           b3 (bit-and (aget hashed 3) 0xFF)]
       ;; Combine into 32-bit unsigned value
       (bit-or (bit-shift-left b0 24)
               (bit-shift-left b1 16)
               (bit-shift-left b2 8)
               b3))))

(defn- value->bytes
  "Convert a value to bytes for hashing"
  [v]
  #?(:clj
     (cond
       (string? v) (.getBytes ^String v "UTF-8")
       (int? v) (-> (ByteBuffer/allocate 8) (.putLong v) .array)
       (double? v) (-> (ByteBuffer/allocate 8) (.putDouble v) .array)
       :else (.getBytes (pr-str v) "UTF-8"))
     :cljs
     (cond
       (string? v) (crypt/stringToUtf8ByteArray v)
       (number? v) (crypt/stringToUtf8ByteArray (str v))
       :else (crypt/stringToUtf8ByteArray (pr-str v)))))

(defn hash-value
  "Hash any value to a 32-bit unsigned integer.

  NOTE: Callers should pass canonicalized values (per QUERY_STATS_AND_HLL.md).
  For proper cardinality estimation, ensure values are in canonical form before hashing."
  [v]
  (-> v value->bytes hash-bytes))

;; Leading zeros counting
(defn- rho
  "Calculate ρ(w) - position of leftmost 1-bit in remaining hash (1-indexed).

  Uses MSB (most significant bits) for index to reduce correlation.
  Index is extracted from top p bits, remaining lower bits feed rho.

  For a 32-bit hash with p=8:
  - Top 8 bits: index (0-255)
  - Lower 24 bits: rho calculation

  Returns position of first 1-bit in lower bits (1-indexed), or 25 if all zeros."
  [hash-val index-bits]
  (let [;; Create mask for lower bits after removing top p bits (32-bit safe)
        remaining-bits-mask (dec (bit-shift-left 1 (- 32 index-bits)))
        remaining-bits (bit-and hash-val remaining-bits-mask)
        max-width (- 32 index-bits)]
    (if (zero? remaining-bits)
      (inc max-width)  ; All zeros: return max+1 (e.g., 25 for p=8)
      (let [lz #?(:clj (Integer/numberOfLeadingZeros remaining-bits)
                  :cljs (js/Math.clz32 remaining-bits))]
        ;; Leading zeros counted in 32-bit value
        ;; We need to count within the lower (32-p) bits
        ;; Subtract the high bits that are zero due to masking
        (inc (- lz index-bits))))))

;; Sketch operations
(defn create-sketch
  "Create a new empty HLL sketch with 256 registers.

  Returns byte-array (CLJ) or Uint8Array (CLJS).
  Register values are 0-63 (unsigned, 6 bits)."
  []
  #?(:clj (byte-array num-registers)
     :cljs (js/Uint8Array. num-registers)))

(defn add-value
  "Add a value to the HLL sketch. Returns updated sketch.

  Algorithm:
  1. Hash the value to 32-bit integer
  2. Use first p bits as register index (0-255)
  3. Count leading zeros in remaining bits + 1
  4. Update register with max(current, leading-zeros)"
  [sketch v]
  (let [h (hash-value v)
        ;; Extract register index from FIRST (MSB) p bits
        ;; This reduces correlation between index and rho
        idx (unsigned-bit-shift-right h (- 32 precision))
        ;; Calculate ρ(w) - position of first 1-bit in remaining lower bits
        rho-val (rho h precision)
        ;; Cap at max register value (6 bits = max 63)
        capped-rho (min rho-val register-mask)
        ;; Get current value and compute max
        current #?(:clj (aget ^bytes sketch idx)
                   :cljs (aget sketch idx))
        new-val (max current capped-rho)]
    ;; Update register with max
    #?(:clj (aset-byte sketch idx (byte new-val))
       :cljs (aset sketch idx new-val))
    sketch))

(defn merge-sketches
  "Merge two HLL sketches using register-wise maximum.
  Returns a new sketch."
  [sketch-a sketch-b]
  (let [result (create-sketch)]
    (dotimes [i num-registers]
      (let [max-val #?(:clj (max (aget ^bytes sketch-a i) (aget ^bytes sketch-b i))
                       :cljs (max (aget sketch-a i) (aget sketch-b i)))]
        #?(:clj (aset-byte result i (byte max-val))
           :cljs (aset result i max-val))))
    result))

(defn- harmonic-mean
  "Calculate harmonic mean for HLL cardinality estimation"
  [registers]
  (let [sum (reduce (fn [acc reg]
                      (+ acc (Math/pow 2.0 (- reg))))
                    0.0
                    registers)]
    (/ 1.0 sum)))

(defn cardinality
  "Estimate cardinality (NDV) from HLL sketch.

  Uses HyperLogLog++ algorithm with bias correction:
  - Small range correction for estimates < 2.5m
  - Large range correction for estimates > 2^32/30
  - Standard formula otherwise"
  [sketch]
  (let [;; Raw estimate
        raw-estimate (* alpha-256
                        num-registers
                        num-registers
                        (harmonic-mean (seq sketch)))

        ;; Small range correction (check for empty registers)
        empty-registers (reduce (fn [acc reg]
                                  (if (zero? reg) (inc acc) acc))
                                0
                                (seq sketch))

        small-corrected (if (and (< raw-estimate small-range-threshold)
                                 (pos? empty-registers))
                          (* num-registers
                             (Math/log (/ num-registers
                                          (double empty-registers))))
                          raw-estimate)

        ;; Large range correction
        two-pow-32 4294967296.0  ; 2^32 as literal for CLJS compat
        final-estimate (if (> small-corrected large-range-threshold)
                         (* -1 two-pow-32
                            (Math/log (- 1.0 (/ small-corrected two-pow-32))))
                         small-corrected)]

    (max 0 (long (Math/round (double final-estimate))))))

;; Serialization
(defn serialize
  "Serialize HLL sketch to base64-encoded string"
  [sketch]
  #?(:clj
     (.encodeToString (java.util.Base64/getEncoder) sketch)
     :cljs
     (base64/encodeByteArray sketch)))

(defn deserialize
  "Deserialize HLL sketch from base64-encoded string.

  Returns byte-array (CLJ) or Uint8Array (CLJS) for stricter semantics
  since register values are 0-63 (unsigned)."
  [base64-str]
  #?(:clj
     (.decode (java.util.Base64/getDecoder) ^String base64-str)
     :cljs
     (let [arr (base64/decodeStringToByteArray base64-str)]
       (js/Uint8Array. arr))))

;; Utility functions
(defn sketch-info
  "Return debug information about a sketch"
  [sketch]
  {:num-registers num-registers
   :precision precision
   :cardinality (cardinality sketch)
   :empty-registers (count (filter zero? (seq sketch)))
   :max-register (apply max (seq sketch))
   :avg-register (/ (reduce + (seq sketch)) num-registers)})
