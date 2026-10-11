CREATE TABLE entities(kind text NOT NULL,id text NOT NULL,revision bigint NOT NULL,data jsonb NOT NULL,PRIMARY KEY(kind,id));
CREATE TABLE etl_batches(batch_no bigserial PRIMARY KEY,status text NOT NULL DEFAULT 'claimed',created_at timestamptz NOT NULL DEFAULT now(),published_at timestamptz,json_sha256 text,delta_manifest jsonb,error text,extract_seconds double precision,transform_seconds double precision,load_seconds double precision);
CREATE TABLE change_queue(event_id bigserial PRIMARY KEY,kind text NOT NULL,id text NOT NULL,revision bigint NOT NULL,op text NOT NULL,data jsonb NOT NULL,source_time timestamptz NOT NULL DEFAULT clock_timestamp(),batch_no bigint REFERENCES etl_batches);
CREATE FUNCTION capture_change() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE r entities;
BEGIN
 IF TG_OP='DELETE' THEN r:=OLD; ELSE r:=NEW; END IF;
 INSERT INTO change_queue(kind,id,revision,op,data) VALUES(r.kind,r.id,r.revision,CASE WHEN TG_OP='DELETE' THEN 'd' ELSE 'u' END,r.data);
 IF TG_OP='DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
END $$;
CREATE TRIGGER entity_changes AFTER INSERT OR UPDATE OR DELETE ON entities FOR EACH ROW EXECUTE FUNCTION capture_change();
CREATE INDEX unclaimed_changes ON change_queue(event_id) WHERE batch_no IS NULL;
