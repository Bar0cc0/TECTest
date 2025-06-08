-- Create schema for TEC data staging tables

CREATE SCHEMA IF NOT EXISTS staging;

-- Drop existing tables first to ensure clean creation
DROP TABLE IF EXISTS staging.operational_capacity CASCADE;

-- Operational Capacity Table
CREATE TABLE IF NOT EXISTS staging.operational_capacity (
    id SERIAL PRIMARY KEY,
    loc TEXT,                       -- Location ID
    loc_zn TEXT,                    -- Location zone
    loc_name TEXT,                  -- Location name
    loc_purp_desc TEXT,             -- Location purpose description
    loc_qti TEXT,                   -- Location quantity type indicator
    flow_ind TEXT,                  -- Flow indicator
    dc NUMERIC,                     -- Design capacity
    opc NUMERIC,                    -- Operating capacity
    tsq NUMERIC,                    -- Total scheduled quantity
    oac NUMERIC,                    -- Operationally available capacity
    it TEXT,                        -- Interruptible indicator
    auth_overrun_ind TEXT, 	        -- Authorized overrun indicator
    nom_cap_exceed_ind TEXT,        -- Nominal capacity exceeded indicator
    all_qty_avail TEXT,             -- All quantity available
    qty_reason TEXT,                -- Quantity reason
    measure_date DATE,              -- Measure date
	cycle_id TEXT,                  -- Cycle identifier
    download_timestamp DOUBLE PRECISION, -- Download timestamp
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Record creation timestamp
);

-- Add constraints to ensure data integrity
ALTER TABLE operational_capacity
    ADD CONSTRAINT chk_op_capacity CHECK (dc >= 0 AND opc >= 0 AND tsq >= 0 AND oac >= 0);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_op_capacity_download_ts ON operational_capacity (download_timestamp);
CREATE INDEX IF NOT EXISTS idx_op_capacity_cycle ON operational_capacity (cycle_id);
CREATE INDEX IF NOT EXISTS idx_op_capacity_loc_id ON operational_capacity (loc);
CREATE INDEX IF NOT EXISTS idx_op_capacity_measure_date ON operational_capacity (measure_date);