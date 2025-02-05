-- Test 1: Basic entropy calculation
CREATE TABLE tbl(a VARCHAR, b VARCHAR, c VARCHAR);
INSERT INTO tbl VALUES ('a1', 'b1', 'c1'), ('a1', 'b2', 'c2'), ('a1', 'b3', 'c2');
SELECT get_entropy(custom_sum(lift([a, b, c]))) FROM tbl;
SELECT get_entropy(sum_no_lift([a, b, c])) FROM tbl;

-- Test 2: Exact lifter 
CREATE TABLE tbl(col4 VARCHAR, col17 VARCHAR, col9 VARCHAR, col13 VARCHAR);
INSERT INTO tbl VALUES ('a1', 'b1', 'c1', 'd1'), ('a1', 'b2', 'c2', 'd2'), ('a1', 'b3', 'c2', 'd3');
SELECT lift_exact([col17, col4, col9, col13], 1) FROM tbl;
SELECT lift_exact([col17, col4, col9, col13], 2) FROM tbl;
SELECT lift_exact([col17, col4, col9, col13], 3) FROM tbl;
SELECT lift_exact([col17, col4, col9, col13], 4) FROM tbl;

-- Test 3: Lifting work 
CREATE TABLE tbl(col1 VARCHAR, col2 VARCHAR, col5 VARCHAR);
INSERT INTO tbl VALUES ('a1', 'b1', 'c1'), ('a1', 'b2', 'c2'), ('a1', 'b3', 'c2'), ('a2', 'b1', 'c1'), ('a2', 'b2', 'c2');
SELECT lift_exact([col1, col2, col5], 1) FROM tbl;
SELECT lift_exact([col1, col2, col5], 2) FROM tbl;
SELECT lift_exact([col1, col2, col5], 3) FROM tbl;


-- Hash list testing
DROP TABLE tbl;
CREATE TABLE tbl(col0 VARCHAR, col1 VARCHAR, col2 VARCHAR);
INSERT INTO tbl VALUES
('a2', 'b3', 'c1'),
('a2', 'b1', 'c2'),
('a2', 'b1', 'c2'),
('a2', 'b3', 'c4'),
('a3', 'b4', 'c7');

-- Level 1 query: A, B, C
CREATE TABLE l1 AS SELECT sum_dict([
    hash_list([col0]), -- A
    hash_list([col1]), -- B
    hash_list([col2])  -- C
]) AS out
FROM tbl;

-- Level 2 query: AB, AC, BC
CREATE TABLE l2 AS SELECT sum_dict([
    -- AB
    CASE
        WHEN filt(hash_list([col0]), l1.out.sets, 0) AND 
             filt(hash_list([col1]), l1.out.sets, 1) 
        THEN hash_list([col0, col1])
        ELSE NULL
    END,
    -- AC
    CASE
        WHEN filt(hash_list([col0]), l1.out.sets, 0) AND 
             filt(hash_list([col2]), l1.out.sets, 2) 
        THEN hash_list([col0, col2])
        ELSE NULL
    END,
    -- BC
    CASE
        WHEN filt(hash_list([col1]), l1.out.sets, 1) AND 
             filt(hash_list([col2]), l1.out.sets, 2) 
        THEN hash_list([col1, col2])
        ELSE NULL
    END
]) AS out
FROM tbl, l1;

-- Level 3 query: ABC 
SELECT sum_dict([
    CASE
        WHEN filt(hash_list([col0, col1]), l2.out.sets, 0) AND 
             filt(hash_list([col0, col2]), l2.out.sets, 1) AND 
             filt(hash_list([col1, col2]), l2.out.sets, 2) THEN hash_list([col0, col1, col2])
        ELSE NULL 
    END
]) FROM tbl, l2;

-- Queries using FILTER syntax
-- Level 2 query: AB, AC, BC
CREATE TABLE l2 AS SELECT ([

]) AS out
FROM tbl, l1;
