CREATE SCHEMA IF NOT EXISTS course_admin;

CREATE TABLE IF NOT EXISTS course_admin.platform_info (
    key text PRIMARY KEY,
    value text NOT NULL
);

INSERT INTO course_admin.platform_info (key, value)
VALUES
    ('platform', 'DSAIT4000 quantum data lake'),
    ('purpose', 'Student relational target; business schemas are student-designed')
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;

COMMENT ON SCHEMA course_admin IS
    'Platform metadata only. Assignment tables should use a separate schema.';

