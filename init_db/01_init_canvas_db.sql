-- Initialize Canvas Data Database
-- This script will be run automatically when PostgreSQL container starts

-- Create extensions if needed
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- Create schema for Canvas data (optional)
CREATE SCHEMA IF NOT EXISTS canvas;

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA canvas TO postgres;
GRANT ALL PRIVILEGES ON SCHEMA public TO postgres;

-- Create indexes after tables are created (will be handled by pandas/SQLAlchemy)
-- These are example indexes that might be useful:

-- Function to create indexes after data load
CREATE OR REPLACE FUNCTION create_canvas_indexes()
RETURNS void AS $$
BEGIN
    -- Users table indexes
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'canvas_users') THEN
        CREATE INDEX IF NOT EXISTS idx_canvas_users_id ON canvas_users(id);
        CREATE INDEX IF NOT EXISTS idx_canvas_users_email ON canvas_users(login_id);
    END IF;

    -- Courses table indexes
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'canvas_courses') THEN
        CREATE INDEX IF NOT EXISTS idx_canvas_courses_id ON canvas_courses(id);
        CREATE INDEX IF NOT EXISTS idx_canvas_courses_account_id ON canvas_courses(account_id);
    END IF;

    -- Assignments table indexes
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'canvas_assignments') THEN
        CREATE INDEX IF NOT EXISTS idx_canvas_assignments_id ON canvas_assignments(id);
        CREATE INDEX IF NOT EXISTS idx_canvas_assignments_course_id ON canvas_assignments(course_id);
    END IF;

    -- Enrollments table indexes
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'canvas_course_enrollment') THEN
        CREATE INDEX IF NOT EXISTS idx_canvas_enrollment_user_id ON canvas_course_enrollment(user_id);
        CREATE INDEX IF NOT EXISTS idx_canvas_enrollment_course_id ON canvas_course_enrollment(course_id);
    END IF;

    RAISE NOTICE 'Canvas indexes created successfully';
END;
$$ LANGUAGE plpgsql;

-- Create a view for basic analytics
CREATE OR REPLACE VIEW canvas_summary AS
SELECT
    'Users' as table_name,
    COUNT(*) as record_count,
    NOW() as last_updated
FROM canvas_users
WHERE EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'canvas_users')

UNION ALL

SELECT
    'Courses' as table_name,
    COUNT(*) as record_count,
    NOW() as last_updated
FROM canvas_courses
WHERE EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'canvas_courses')

UNION ALL

SELECT
    'Assignments' as table_name,
    COUNT(*) as record_count,
    NOW() as last_updated
FROM canvas_assignments
WHERE EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'canvas_assignments');

COMMENT ON VIEW canvas_summary IS 'Summary view showing record counts for main Canvas tables';
