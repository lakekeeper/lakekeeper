-- Factors project into a separate table.
-- Previously projects were nameless and only existed as part of a warehouse.
create table project
(
    project_id uuid primary key,
    project_name text not null
);

call add_time_columns('project');
select trigger_updated_at('"project"');

INSERT INTO project (project_id, project_name)
SELECT warehouse.project_id, 'Unnamed Project' FROM warehouse;

alter table warehouse
    add constraint warehouse_project_id_fk foreign key (project_id) references project (project_id) on update cascade;
