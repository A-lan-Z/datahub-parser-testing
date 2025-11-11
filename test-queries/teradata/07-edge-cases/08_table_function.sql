-- Tests table-valued functions (limitation area)
-- Expected: May fail to track lineage through table functions
-- Expected: Function result may not be resolved to underlying tables

SELECT
    result.customer_id,
    result.customer_name,
    result.segment_name,
    result.segment_score
FROM TABLE(customer_segmentation_function(
    DATE '2024-01-01',
    DATE '2024-12-31'
)) AS result
WHERE result.segment_score > 80;
