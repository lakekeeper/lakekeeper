-- Valid migration; the next migration in this fixture deliberately fails to
-- exercise the all-or-nothing atomicity guarantee of `migrate(pool, ...)`.
CREATE TABLE ext_demo_atomic (
    id UUID PRIMARY KEY
);
