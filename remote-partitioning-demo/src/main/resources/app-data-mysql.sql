-- 10 rows, ids 1..10. With gridSize=3 the ColumnRangePartitioner splits this into
-- ranges [1..4], [5..8], [9..10] — one per worker partition.
INSERT INTO PEOPLE (ID, NAME) VALUES
 (1,'person-01'),(2,'person-02'),(3,'person-03'),(4,'person-04'),(5,'person-05'),
 (6,'person-06'),(7,'person-07'),(8,'person-08'),(9,'person-09'),(10,'person-10');
