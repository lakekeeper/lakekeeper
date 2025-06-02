alter table view_version
    alter column version_id type int4;
alter table current_view_metadata_version
    alter column version_id type int4;
alter table view_version_log
    alter column version_id type int4;
alter table view_representation
    alter column view_version_id type int4;