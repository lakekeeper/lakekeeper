-- Add migration script here
CREATE OR REPLACE FUNCTION get_stats_date_default() RETURNS timestamptz AS
$$
BEGIN
    RETURN date_trunc(get_stats_interval_unit(), now()) + get_stats_interval();
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_stats_interval_unit() RETURNS text AS
$$
BEGIN
    RETURN 'hour';
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_stats_interval() RETURNS interval AS
$$
BEGIN
    RETURN interval '1 ' || get_stats_interval_unit();
END;
$$ LANGUAGE plpgsql;

create table warehouse_statistics_history
(
    number_of_views  bigint      not null,
    number_of_tables bigint      not null,
    warehouse_id     uuid        not null REFERENCES warehouse (warehouse_id) ON DELETE CASCADE,
    timestamp        timestamptz not null default get_stats_date_default(),
    primary key (warehouse_id, timestamp)
);


CREATE UNIQUE INDEX idx_warehouse_stats_history_time
    ON warehouse_statistics_history (warehouse_id, timestamp DESC);

INSERT INTO warehouse_statistics_history (number_of_views, number_of_tables, warehouse_id, timestamp)
SELECT number_of_views,
       number_of_tables,
       warehouse_id,
       timestamp
FROM warehouse_statistics;

call add_time_columns('warehouse_statistics_history');
select trigger_updated_at('warehouse_statistics_history');

drop table if exists warehouse_statistics;

create table warehouse_statistics
(
    warehouse_id     uuid primary key not null REFERENCES warehouse (warehouse_id) ON DELETE CASCADE,
    number_of_views  bigint           not null,
    number_of_tables bigint           not null,
    timestamp        timestamptz      not null default get_stats_date_default()
);

call add_time_columns('warehouse_statistics');
select trigger_updated_at('warehouse_statistics');

-- initial count
insert into warehouse_statistics (number_of_views, number_of_tables, warehouse_id)
select count(*) filter (where t.typ = 'view'),
       count(*) filter (where t.typ = 'table'),
       n.warehouse_id
from tabular t
         join namespace n on t.namespace_id = n.namespace_id
group by n.warehouse_id;


-- this doesn't consider soft-deletes, soft-deletes are setting deleted_at instead of deleting the row,
create or replace function update_counts() returns trigger as
$$
declare
    delta_view             integer;
    delta_table            integer;
    coalesced_namespace_id uuid;
    coalesced_type         tabular_type;
    truncated_date         timestamptz;
    old_views              integer;
    old_tables             integer;
    old_ts                 timestamptz;
    target_wh              uuid;
begin
    -- COALESCE here is to handle the case when the row is being deleted and NEW is null
    coalesced_type := COALESCE(NEW.typ, OLD.typ);
    coalesced_namespace_id := COALESCE(NEW.namespace_id, OLD.namespace_id);
    truncated_date := get_stats_date_default();

    delta_view := CASE
                      WHEN coalesced_type = 'view' THEN
                          CASE
                              WHEN TG_OP = 'INSERT' THEN 1
                              WHEN TG_OP = 'DELETE' THEN -1
                              ELSE 0
                              END
                      ELSE 0
        END;
    delta_table := CASE
                       WHEN coalesced_type = 'table' THEN
                           CASE
                               WHEN TG_OP = 'INSERT' THEN 1
                               WHEN TG_OP = 'DELETE' THEN -1
                               ELSE 0 END
                       ELSE 0
        END;

    if delta_view = 0 and delta_table = 0 then
        return null;
    end if;

    WITH old_data AS (SELECT timestamp, number_of_views, number_of_tables, warehouse_id
                      FROM warehouse_statistics
                      WHERE warehouse_id =
                            (SELECT warehouse_id FROM namespace WHERE namespace_id = coalesced_namespace_id))
    UPDATE warehouse_statistics ws
    SET number_of_views  = GREATEST(number_of_views + delta_view, 0),
        number_of_tables = GREATEST(number_of_tables + delta_table, 0),
        timestamp        = truncated_date
    WHERE ws.warehouse_id = (SELECT warehouse_id FROM namespace WHERE namespace_id = coalesced_namespace_id)
    RETURNING (SELECT timestamp FROM old_data) AS old_timestamp,
            (SELECT number_of_views FROM old_data) AS old_views,
            (SELECT number_of_tables FROM old_data) AS old_tables,
            (SELECT warehouse_id FROM old_data) AS warehouse_id
        INTO old_ts, old_views, old_tables, target_wh;

    if old_ts < truncated_date then
        INSERT INTO warehouse_statistics_history (number_of_views,
                                                  number_of_tables,
                                                  warehouse_id,
                                                  timestamp)
        SELECT old_views,
               old_tables,
               target_wh,
               old_ts
        ON CONFLICT DO NOTHING;
    end if;

    RETURN NULL;
end;
$$ language plpgsql;


-- we only do insert or delete since we don't support moving tabular between warehouses and stats are warehouse level
DROP TRIGGER IF EXISTS update_counts
    ON tabular;
CREATE CONSTRAINT TRIGGER update_counts
    AFTER INSERT OR DELETE
    ON tabular
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW
EXECUTE PROCEDURE update_counts();

DROP TRIGGER IF EXISTS update_counts_trunc
    ON tabular;
-- TRUNCATE triggers must be FOR EACH STATEMENT
CREATE TRIGGER update_counts_trunc
    AFTER TRUNCATE
    ON tabular
    FOR EACH STATEMENT
EXECUTE PROCEDURE update_counts();

