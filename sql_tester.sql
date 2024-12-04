
-- \c test_db;

-- \timing
DROP TABLE IF EXISTS table1;
CREATE TABLE table1 (
    key1 INTEGER,
    key2 INTEGER,
    text_data CHAR(1),
    value1 NUMERIC,
    value2 NUMERIC,
    value3 TEXT,
    timestamp TIMESTAMP
);

DROP TABLE IF EXISTS table2;
CREATE TABLE table2 (
    key1 INTEGER,
    key2 INTEGER,
    metric1 NUMERIC,
    metric2 NUMERIC,
    metric3 TEXT,
    category CHAR(1),
    date TIMESTAMP
);
-- Measure runtime for the join operation
DO $$
DECLARE
    start_time TIMESTAMP;
    end_load_time TIMESTAMP;
    end_join_load_time TIMESTAMP;
BEGIN
    -- Record start time
    start_time := clock_timestamp();
    
    -- Load CSV into tables using COPY
    COPY table1 FROM '/Users/michellewang/Desktop/pandasql/data/A.csv' CSV HEADER;
    COPY table2 FROM '/Users/michellewang/Desktop/pandasql/data/B.csv' CSV HEADER;
    
    end_load_time := clock_timestamp();

    -- Join the two tables
    PERFORM * 
    FROM table1
    JOIN table2
    ON table1.key1 = table2.key1;

    -- Record end time
    end_join_load_time := clock_timestamp();

    -- Print runtime
    RAISE NOTICE 'Total Runtime: % seconds', EXTRACT(EPOCH FROM (end_join_load_time - start_time));
    RAISE NOTICE 'Loading Runtime: % seconds', EXTRACT(EPOCH FROM (end_load_time - start_time));
    RAISE NOTICE 'Joining Runtime: % seconds', EXTRACT(EPOCH FROM (end_join_load_time - end_load_time));
END $$;




-- -- Record start time for loading data
-- SELECT clock_timestamp() AS start_time \gset

-- -- Load CSV data into tables
-- COPY table1 FROM '/data/A.csv' CSV HEADER;
-- COPY table2 FROM '/data/B.csv' CSV HEADER;

-- -- Record end time after loading data
-- SELECT clock_timestamp() AS end_load_time \gset

-- -- Record start time for join operation
-- SELECT clock_timestamp() AS start_join_time \gset

-- -- Perform the join operation (you can store the result if needed)
-- SELECT COUNT(*) FROM table1
-- JOIN table2 ON table1.key1 = table2.key1;

-- -- Record end time after join operation
-- SELECT clock_timestamp() AS end_join_time \gset

-- \echo :start_time
-- \echo :end_load_time
-- \echo :start_join_time
-- \echo :end_join_time

-- -- Calculate and display runtimes
-- SELECT 'Total Runtime: ' || EXTRACT(EPOCH FROM (:end_join_time::timestamp - :start_time::timestamp)) || ' seconds' AS total_runtime;
-- SELECT 'Loading Runtime: ' || EXTRACT(EPOCH FROM (':end_load_time'::timestamp - ':start_time'::timestamp)) || ' seconds' AS loading_runtime;
-- SELECT 'Joining Runtime: ' || EXTRACT(EPOCH FROM (':end_join_time'::timestamp - ':start_join_time'::timestamp)) || ' seconds' AS joining_runtime;

-- \timing