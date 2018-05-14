INVALIDATE METADATA devicereadings;

SELECT id, max(temperature) from devicereadings GROUP BY id;

SELECT id, count(*) from devicereadings GROUP BY id;

SELECT to_date(time), id, max(temperature) from devicereadings GROUP BY to_date(time), id ORDER BY to_date(time), id;
