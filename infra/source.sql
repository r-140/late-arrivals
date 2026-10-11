CREATE TABLE entities (
 kind text NOT NULL, id text NOT NULL, revision bigint NOT NULL,
 data jsonb NOT NULL, PRIMARY KEY(kind,id));
ALTER TABLE entities REPLICA IDENTITY FULL;
-- revision is an explicit per-entity monotonic application version.
