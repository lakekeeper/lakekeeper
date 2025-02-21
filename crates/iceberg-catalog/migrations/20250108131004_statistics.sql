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

create table warehouse_statistics
(
    stats_id         uuid primary key     default uuid_generate_v1mc(),
    number_of_views  bigint      not null,
    number_of_tables bigint      not null,
    warehouse_id     uuid        not null REFERENCES warehouse (warehouse_id) ON DELETE CASCADE,
    timestamp        timestamptz not null default get_stats_date_default(),
    CONSTRAINT positive_counts
        CHECK (number_of_views >= 0 AND number_of_tables >= 0)
);

call add_time_columns('warehouse_statistics');
select trigger_updated_at('warehouse_statistics');

CREATE UNIQUE INDEX idx_warehouse_stats_time
    ON warehouse_statistics (warehouse_id, timestamp DESC);

-- this doesn't consider soft-deletes, soft-deletes are setting deleted_at instead of deleting the row,
create or replace function update_counts() returns trigger as
$$
declare
    delta_view             integer;
    delta_table            integer;
    coalesced_namespace_id uuid;
    coalesced_type         tabular_type;
    truncated_date         timestamptz;
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

    INSERT
    INTO warehouse_statistics (number_of_views,
                               number_of_tables,
                               warehouse_id,
                               timestamp)
    SELECT number_of_views + delta_view,
           number_of_tables + delta_table,
           ws.warehouse_id,
           truncated_date
    FROM warehouse_statistics ws
             JOIN namespace n ON ws.warehouse_id = n.warehouse_id
    WHERE n.namespace_id = coalesced_namespace_id
    ORDER BY ws.timestamp DESC
    LIMIT 1
    ON CONFLICT (warehouse_id, timestamp)
        DO UPDATE SET number_of_views  = EXCLUDED.number_of_views,
                      number_of_tables = EXCLUDED.number_of_tables;

    RETURN NULL;
end;
$$ language plpgsql;


-- we only do insert or delete since we don't support moving tabular between warehouses and stats are warehouse level
CREATE CONSTRAINT TRIGGER update_counts
    AFTER INSERT OR DELETE
    ON tabular
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW
EXECUTE PROCEDURE update_counts();

-- TRUNCATE triggers must be FOR EACH STATEMENT
CREATE TRIGGER update_counts_trunc
    AFTER TRUNCATE
    ON tabular
    FOR EACH STATEMENT
EXECUTE PROCEDURE update_counts();

-- initial count
insert into warehouse_statistics (number_of_views, number_of_tables, warehouse_id)
select count(*) filter (where t.typ = 'view'),
       count(*) filter (where t.typ = 'table'),
       n.warehouse_id
from tabular t
         join namespace n on t.namespace_id = n.namespace_id
group by n.warehouse_id;