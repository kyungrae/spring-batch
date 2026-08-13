-- Sample business table to partition by ID range. Dropped + recreated each run
-- (see spring.sql.init in application-local.yml) so IDs/ranges are deterministic.
DROP TABLE IF EXISTS PEOPLE;
CREATE TABLE PEOPLE (
    ID   BIGINT       NOT NULL PRIMARY KEY,
    NAME VARCHAR(100) NOT NULL
);
