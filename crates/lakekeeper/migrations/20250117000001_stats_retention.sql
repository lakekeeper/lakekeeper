-- Migration to add helper functions for statistics retention
-- This migration adds indexes and functions to optimize statistics cleanup operations

-- Add indexes to improve performance of retention cleanup queries
CREATE INDEX IF NOT EXISTS idx_endpoint_stats_timestamp_cleanup 
    ON endpoint_statistics (timestamp) 
    WHERE timestamp < now() - interval '1 day';

CREATE INDEX IF NOT EXISTS idx_warehouse_stats_history_timestamp_cleanup 
    ON warehouse_statistics_history (timestamp) 
    WHERE timestamp < now() - interval '1 day';

-- Function to clean up old endpoint statistics by age
CREATE OR REPLACE FUNCTION cleanup_endpoint_statistics_by_age(cutoff_timestamp timestamptz)
RETURNS bigint AS $$
DECLARE
    deleted_count bigint;
BEGIN
    DELETE FROM endpoint_statistics 
    WHERE timestamp < cutoff_timestamp;
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Function to clean up old warehouse statistics by age
CREATE OR REPLACE FUNCTION cleanup_warehouse_statistics_by_age(cutoff_timestamp timestamptz)
RETURNS bigint AS $$
DECLARE
    deleted_count bigint;
BEGIN
    DELETE FROM warehouse_statistics_history 
    WHERE timestamp < cutoff_timestamp;
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Function to clean up excess endpoint statistics by count
CREATE OR REPLACE FUNCTION cleanup_endpoint_statistics_by_count(max_entries integer)
RETURNS bigint AS $$
DECLARE
    deleted_count bigint;
BEGIN
    WITH entries_to_delete AS (
        SELECT endpoint_statistics_id
        FROM (
            SELECT 
                endpoint_statistics_id,
                ROW_NUMBER() OVER (
                    PARTITION BY project_id, warehouse_id, matched_path, status_code 
                    ORDER BY timestamp DESC
                ) AS rn
            FROM endpoint_statistics
        ) ranked
        WHERE rn > max_entries
    )
    DELETE FROM endpoint_statistics 
    WHERE endpoint_statistics_id IN (SELECT endpoint_statistics_id FROM entries_to_delete);
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Function to clean up excess warehouse statistics by count
CREATE OR REPLACE FUNCTION cleanup_warehouse_statistics_by_count(max_entries integer)
RETURNS bigint AS $$
DECLARE
    deleted_count bigint;
BEGIN
    WITH entries_to_delete AS (
        SELECT warehouse_id, timestamp
        FROM (
            SELECT 
                warehouse_id,
                timestamp,
                ROW_NUMBER() OVER (
                    PARTITION BY warehouse_id 
                    ORDER BY timestamp DESC
                ) AS rn
            FROM warehouse_statistics_history
        ) ranked
        WHERE rn > max_entries
    )
    DELETE FROM warehouse_statistics_history 
    WHERE (warehouse_id, timestamp) IN (
        SELECT warehouse_id, timestamp FROM entries_to_delete
    );
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql; 