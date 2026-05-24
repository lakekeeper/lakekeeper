-- Deliberately invalid: references a table that does not exist. The whole
-- transaction (core migrations + the prior valid extension migration) must
-- roll back when this fails.
INSERT INTO this_table_does_not_exist (col) VALUES (1);
