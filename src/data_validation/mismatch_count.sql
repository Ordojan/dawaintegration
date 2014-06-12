-- This script will produce the number of mismatches between the old dataset and the new one.
-- It only works on the records that exist in both tables. That means records that only exist in one table won't be considered.

SELECT 
    count(new.doorcount) AS 'mismatches'
FROM
    SAMMY_NEW.SAM_HOUSEUNITS AS new
        INNER JOIN SAMMY.SAM_HOUSEUNITS AS old 
        ON old.kommuneid = new.kommuneid AND new.roadid = old.roadid AND new.HOUSEID = old.houseid
WHERE
    new.doorcount != old.doorcount;
