-- Step 1: Remove foreign key constraints that reference project_id
ALTER TABLE warehouse DROP CONSTRAINT warehouse_project_id_fk;
ALTER TABLE role DROP CONSTRAINT role_project_id_fkey;

-- Step 2: Drop indexes that involve project_id to avoid conflicts
DROP INDEX IF EXISTS unique_role_name_in_project;
DROP INDEX IF EXISTS role_project_id_idx;

-- Step 3: Add temporary text columns to hold the string representation
ALTER TABLE project ADD COLUMN project_id_text text;
ALTER TABLE warehouse ADD COLUMN project_id_text text;
ALTER TABLE role ADD COLUMN project_id_text text;

-- Step 4: Copy UUID values as strings to the temporary columns
UPDATE project SET project_id_text = project_id::text;
UPDATE warehouse SET project_id_text = project_id::text;
UPDATE role SET project_id_text = project_id::text;

-- Step 4b: Make temporary columns NOT NULL after populating them
ALTER TABLE project ALTER COLUMN project_id_text SET NOT NULL;
ALTER TABLE warehouse ALTER COLUMN project_id_text SET NOT NULL;
ALTER TABLE role ALTER COLUMN project_id_text SET NOT NULL;

-- Step 5: Drop primary key constraint from project table
ALTER TABLE project DROP CONSTRAINT project_pkey;

-- Step 6: Drop original UUID columns
ALTER TABLE project DROP COLUMN project_id;
ALTER TABLE warehouse DROP COLUMN project_id;
ALTER TABLE role DROP COLUMN project_id;

-- Step 7: Rename text columns to be the primary columns
ALTER TABLE project RENAME COLUMN project_id_text TO project_id;
ALTER TABLE warehouse RENAME COLUMN project_id_text TO project_id;
ALTER TABLE role RENAME COLUMN project_id_text TO project_id;

-- Step 8: Add back primary key constraint on project table
ALTER TABLE project ADD CONSTRAINT project_pkey PRIMARY KEY (project_id);

-- Step 9: Re-establish foreign key constraints
ALTER TABLE warehouse 
    ADD CONSTRAINT warehouse_project_id_fk 
    FOREIGN KEY (project_id) REFERENCES project (project_id) ON UPDATE CASCADE;

ALTER TABLE role
    ADD CONSTRAINT role_project_id_fkey
    FOREIGN KEY (project_id) REFERENCES project (project_id);

-- Step 10: Recreate indexes
CREATE UNIQUE INDEX unique_role_name_in_project ON role (project_id, (lower(name)));
CREATE INDEX role_project_id_idx ON role (project_id);

-- Step 11: Drop column table_migrated from tabulars
ALTER TABLE tabular DROP COLUMN table_migrated;
