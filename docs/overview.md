# FlureeDB Networks

A FlureeDB network consists of ledger server(s), query server(s), and optionally query clients. In development, or low volume/non-redundant setups, a single server can perform all of the roles. In a redundant or production environment, these roles are served by dedicated processes ideally on redundant, dedicated servers.

## Why Fluree separated query/transaction

In Fluree, the role of a server handling queries (query server) is separated from that providing updates (a ledger server). This serves several purposes:

1) If running decentralized, you don't control all the ledger servers processing updates -- or possibly any. Your apps will require a fast and responsive query engine and by running your own (or a Fluree hosted) query server, you have a dedicated server address to issue queries and coordinate transactions you might send it.

2) This design allows your query servers to linearly scale to whatever query workload you want to throw at it. You can add (or remove) servers as needed and have as much speed and redundancy desired where apps typically need it the most - query. Transactions will in no way affect query performance, and vice versa, because they don't fight for the same resources.

3) This design opens up the ability of running your database as a _library_ inside your own application (in-process). This has implications of how you code, as you ask for data as needed with results in the order of _microseconds_, instead of packaging up queries as monolithic requests to send over the wire for responses in the tens, hundreds, or even thousands of milliseconds. Using this pattern, your code becomes simpler, easier to understand, and more efficient. Fluree very utilizes the resources you give it, and will swap resources in and out of memory as needed. This allows Fluree to operate at in-memory speeds for many databases with just hundreds of MBs of memory resources given to it, making it a fairly lightweight co-pilot to your app.

## The Fluree network

A Fluree instance starts with a ledger group, and a group can consist of as little as 1 ledger server and for a redundant production environment would be 3, or more ideally 5 ledger servers (some scenarios could dictate more). You control all the servers in a ledger group and they act as "one" but share the workload amongst them and provide fault tolerance.

Your apps then talk directly to the next layer, query servers. Query servers can be run as a library inside of your code, or as stand-alone servers exposing both a REST API and a GraphQL API. You can run as many query servers as your workload demands, and they talk directly to your ledger group to get updates and configuration information. Query servers can easily sit behind a load balancer for redundancy, or when running a query peer as a library, this becomes moot as Fluree effectively is now part of your application.

The last layer, query clients, are completely optional, but they allow Fluree to run directly inside your web and mobile apps (or other client apps that support JavaScript). They connect to a query peer, which is then responsible for streaming relevant updates to the query clients as they happen. Using a query client gives your web/mobile apps pretty amazing abilities, as they automatically become real-time and they also can have a "rewind" capability allowing them to time-travel to any historical point in time. You can of course build similar features yourself as the peers are simply exposing capabilities that FlureeDB natively has, but the query client is just a nice packaged reference implementation of it for you to optionally use.

So unlike how you probably think of a 'database', Fluree gives you tools to maximize its capabilities at every tier of your application stack. You can of course relegate it to just the bottom/data tier, this is in fact a great way to get started, but its design gives consideration to how to best serve as an extremely effective data provider to your app as opposed to 'just a database'. Every tier of most applications can be made better with more direct and interactive data provider capabilities.
 
If using all three of these components, the overall network would look something like the following:


```
  Transactor Group    | Scalable Query Servers | Optional Query Clients
     (data tier)      |    (data/app tier)     |    (client tier)
  -----------------------------------------------------------------
                                                        ...
                                  ...             /- Query Client
    ------------        /--- Query Server ---\   /-- Query Client
  /-   Ledger   -\ --\ /---- Query Server ----\ /--- Query Client
 |--   Ledger   --|---X----- Query Server -----X---- Query Client
  \-   Ledger   -/ --/ \---- Query Server ----/ \--- Query Client
    ------------        \--- Query Server ---/   \-- Query Client
                                  ...             \- Query Client
                                                         ...
                                                        
