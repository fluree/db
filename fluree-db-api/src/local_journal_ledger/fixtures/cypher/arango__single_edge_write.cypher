MATCH (n:User {id: $from}), (m:User {id: $to}) WITH n, m
CREATE (n)-[e:Temp]->(m) RETURN e
