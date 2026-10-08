# License FAQ

Fluree DB is licensed under the
[Business Source License 1.1](https://github.com/fluree/db/blob/main/LICENSE)
(BUSL). This page answers common questions about what that allows. It is a
summary: if anything here differs from the license text, the license governs.

## The short version

You may use Fluree DB in production, for free, for almost anything. The one
thing you may not do without a commercial license is offer Fluree itself to
others as a hosted or managed database, which the license calls a *Database
Service*.

Building your own applications and data products on Fluree is permitted, even
when the public can query them.

We always appreciate mentions about your Fluree use or experience, we count on
word of mouth so if you find value, please give a shout out! (and star the repo!) 

## General

### Can I use Fluree DB in production without paying?

Yes. The Additional Use Grant permits production use of any kind, except using
Fluree for a Database Service.

### Why is there a restriction at all?

To prevent a cloud provider or other vendor from taking Fluree and selling it as
their own hosted database service. The restriction is aimed at that and nothing
else. It is not meant to limit companies whose product is their application or
their data.

## What counts as a Database Service

### What is a Database Service?

A commercial offering that gives third parties Fluree itself, or a substantial
portion of its functionality, as a hosted or managed database for their own
purposes. In practice, that means your customers can do one of the following:

- create, provision, or administer their own databases, ledgers, or graphs; or
- define their own schemas, ontologies, or data models and use Fluree as the
  database for their own applications or purposes.

The question is who defines the data model. If you do, Fluree is part of your
product. If your customers do, you are offering them a database.

Your employees, contractors, and service providers acting solely on your behalf
are not third parties.

### What are examples of a Database Service?

- A hosted "Fluree as a service" or "graph database as a service" product.
- A platform where each customer gets their own Fluree ledgers or graphs to load
  and manage.
- A service where customers design their own schemas, load their own data, and
  use Fluree as the database engine behind their own applications.
- A thin wrapper that resells Fluree's query and transaction APIs to customers as
  a general-purpose database under another name.

### What is *not* a Database Service?

- Using Fluree as the database behind your own SaaS application, website, or
  internal system, including when your users query and transact through
  Fluree's APIs directly.
- Publishing a dataset you collect, curate, or license, and letting the public
  query it.
- Running Fluree inside your company for your own teams.

## Building on Fluree

### Can I build a SaaS application on Fluree?

Yes. Your application and its users may use Fluree's APIs fully, including
querying and transacting directly, in SPARQL, JSON-LD, or any other interface
Fluree provides. What matters is that you define the schema or data model for
your application. Your customers use your product; they are not designing their
own databases on top of Fluree.

### Can I publish my own dataset with a public SPARQL endpoint?

Yes, including an endpoint that accepts arbitrary read-only SPARQL queries
written by your users or customers, and including when you charge for access.
When you control the data and your customers only read it, your product is the
data, not the database.

### Can customers' AI agents query my data through MCP tools or an API?

Yes, whether the tools run predefined queries or accept free-form read-only
queries over your data.

### What would turn my data product into a Database Service?

Letting customers create their own ledgers or graphs, or define their own schemas
and use Fluree as the database for their own data and applications. If your
roadmap heads that way, contact us.

### Can I embed Fluree DB in software I distribute to customers?

Yes. The restriction applies to hosted or managed offerings. Fluree DB and any
modified version must still carry the license, and your recipients are bound by
it in turn.

### Can a consultant or contractor run Fluree for my company?

Yes. Service providers acting solely on your behalf are treated as part of your
organization, not as third parties.

### Can I modify Fluree DB?

Yes. You may modify it and create derivative works. Modified versions remain
under BUSL, with the same Additional Use Grant, until their Change Date.

## Change Date and versions

### When does a version become Apache 2.0?

Four years after that version is first publicly distributed. From then on, that
version is available under the Apache License 2.0 with no Database Service
restriction. Each version has its own Change Date.

### Which versions does this FAQ apply to?

Fluree DB 4.0.0 and later. The license text was reworded after the first 4.x
releases to state the Database Service restriction more precisely. The meaning
did not change, and this FAQ reflects how Fluree, PBC applies the license to
every 4.x version.

## Contributing and commercial licensing

### What license do contributions use?

See [Contributing](../contributing/README.md#license). Contributions are licensed
to Fluree, PBC under the Apache License 2.0.

### What if my use is a Database Service, or I'm not sure?

Contact Fluree through [flur.ee](https://flur.ee/). We offer commercial licenses
and are happy to tell you in writing whether your use is covered.
