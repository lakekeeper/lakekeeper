alter table task
    add column state jsonb;

update task
set state = jsonb_build_object(
        'tabular_id', te.tabular_id,
        'typ', te.typ,
        'deletion_kind', te.deletion_kind)
from tabular_expirations te
where te.task_id = task.task_id;

update task
set state = jsonb_build_object(
        'tabular_id', tp.tabular_id,
    -- TODO: check what happens here and if we need a cast to text
        'typ', tp.typ,
        'tabular_location', tp.tabular_location)
from tabular_purges tp
where tp.task_id = task.task_id;

alter table tabular
    add column expiration_task_id uuid references task (task_id) on delete set null,
    add column purge_task_id      uuid references task (task_id) on delete set null;

update tabular
set expiration_task_id = te.task_id
from tabular_expirations te
where te.tabular_id = tabular.tabular_id;

update tabular
set purge_task_id = tp.task_id
from tabular_purges tp
where tp.tabular_id = tabular.tabular_id;

drop table tabular_expirations;
drop table tabular_purges;

alter table task
    alter column state set not null;

