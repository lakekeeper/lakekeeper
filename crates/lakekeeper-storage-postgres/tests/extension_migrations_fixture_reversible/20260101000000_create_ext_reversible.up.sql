-- A reversible extension migration: `migrate` applies the up file and never the down file.
CREATE TABLE ext_reversible_state (id UUID PRIMARY KEY);
