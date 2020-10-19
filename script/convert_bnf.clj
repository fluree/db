#! /usr/bin/env bb

(def cli-options
  [["-i" "--input PATH" "Input Path"]
   ["-o" "--output PATH" "Output Path"]])

(defn inner-string [s]
  (let [end (- (count s) 1)]
    (subs s 1 end)))

(defn rule-name-sanitizer [nme]
  (-> nme
      str/lower-case
      inner-string
      (str/replace " ", "-")))

(defn sanitize-rule-names [line]
  (str/replace line #"<[^>]+>" rule-name-sanitizer))

(defn not-comment? [line]
  (not (str/starts-with? line "--")))

(defn remove-leading-slashes [line]
  (if (str/starts-with? line "//")
    (subs line 2)
    line))

(defn replace-dots [line]
  (str/replace line "'...'" " +"))

(defn process-file [in-path out-path]
  (with-open [r (io/reader in-path)
              w (io/writer out-path)]
    (->> r
         line-seq
         (filter not-comment?)
         (map remove-leading-slashes)
         (map replace-dots)
         (map sanitize-rule-names)
         (map #(.write w (str % "\n")))
         dorun)))

(let [cli (tools.cli/parse-opts *command-line-args* cli-options)
      in-path (-> cli :options :input)
      out-path (-> cli :options :output)]
  (process-file in-path out-path))
