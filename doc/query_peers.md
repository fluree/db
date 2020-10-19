# Query Peers Library

## Connecting

Query peers can connect to one or multiple FlureeDB transactor groups, which may be participating in one or more FlureeDB networks (networks are a grouping of databases, sort of like a top level domain name). FlureeDB has the ability to reference subjects across networks, effectively performing joins across databases that are operated completely independently.

### Creating a connection to a transactor group

The first step in establishing a query peer is to establish a 'connection' to your transactor group. Because you may be running multiple transactors in your group, you can provide a list of addresses and the query peer will pick one at random to be its primary. If disconnected, it will re-try a different transactor. Establishing a connection is done as follows, with both the protocol and port being optional (tcp:// and port 9790 are used as defaults):

```clojure
(def conn (fdb/connect "tcp://10.1.1.1:9790,tcp://10.0.0.0:9790,tcp://10.2.2.2:9790"))
```

### Creating a database session

With a connection established, you create a 'session' to a specific database served by that network. Sessions are cached internally, so you can create them at-will and do not need to store them centrally. To create a session, you pass it a connection and the name of the database you wish to interact with. A database can be referenced with `<network>/<db-name>`.

```clojure
(def mydb-sess (fdb/session conn "my_network/mydb"))
```

A session will automatically keep the current version of the database up-to-date for you through its communication with the transactor group. Once idle for a period of time, a session will close to save resources. It will automatically re-load the next time you use it.

Sessions are used to access any version of the database (both current and historical), in addition it is used to send transactions (updates) for processing.

### Querying a database

To get the most current known version of the database to query, we request it from the session like:

```clojure
(def mydb (fdb/db mydb-sess))
``` 

Here, `mydb` is  contains an immutable version of the mydb database as of the moment in time you asked for it. It can be passed around your application, and will never, ever, ever, ever (is that clear?) change underneath you. If you had a long-running process and wanted to keep getting the most current version of the database, just ask for it again from the session using `fdb/db`.

To query `mydb`:

```clojure
(fdb/query mydb {:select ["*"] :from "_collection"})
```

To get a historical version of the database "mydb", you specify the moment in time either using the 'block' number or using a wall clock time with an ISO-8601 string. 

```clojure
;; using a block to represent the moment in time:
(def old-mydb (fdb/db mydb-sess 10))
;; using a time string to represent the moment in time:
(def old-mydb (fdb/db mydb-sess "2018-08-28T14:43:21.440Z"))
```

### Transacting on a database

Transactions always work against the most recent version of the database and are processed in order. The transactors are the only servers who have the guarantee of the ordering and most recent db version. Therefore, we send all transactions to the transactor group for processing, and they will return a response.

We use the database session to issue transactions and handle this communication for us. Here is a sample transaction:

```clojure
(db/transact mydb-sess [{:_id "person$tempid" :name "John Smith"}])
```

Blockchain transactions in reality are not this simple, they require the transaction to be signed using a private key, and possibly other metadata. A private key can be provided when establishing the session which will be used as a default for transactions. If the session does not have a key specified, a default for the original connection, if specified, is passed to each session.

Each individual transaction can also specify a private key, or it can simply relay an already signed transaction that presumably was created in a client somewhere. Regardless of how it happens, by the time a transaction gets to the transactor group, it is signed and locked. The transactor group will only process it if valid.

