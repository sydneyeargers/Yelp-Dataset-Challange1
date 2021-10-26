--avg checkins per week per zipcode (THIS WORKS!)
SELECT 	RANK() OVER(PARTITION BY bn.postal_code ORDER BY (coalesce(num_checkins,0) - avg_checkins) desc) as Rank, 
	name, cast(coalesce(num_checkins,0) as varchar(6)) as num_checkins, cast(stars as varchar(5))
FROM business as bn
INNER JOIN (SELECT postal_code, avg(num_checkins) as avg_checkins
			FROM business as b
			GROUP BY postal_code
			ORDER BY postal_code) as temp 
			ON bn.postal_code = temp.postal_code
WHERE bn.postal_code = '89103'
ORDER BY rank;

-- create popular_businesses table
DROP TABLE popular_businesses;
CREATE TABLE POPULAR_BUSINESSES(
	pop_number INT,
	business_id VARCHAR(24),
	postal_code CHAR(10),
	difference decimal(10,2)
);
-- trying to insert data into pop_table:
INSERT INTO popular_businesses(business_id, postal_code)
SELECT business_id, postal_code 
FROM Business;

UPDATE popular_businesses 
	SET pop_number = (SELECT RANK() OVER(PARTITION BY bn.postal_code ORDER BY (coalesce(num_checkins,0) - avg_checkins) desc) as Rank
					  FROM business as bn
					  INNER JOIN (SELECT postal_code, avg(num_checkins) as avg_checkins
								  FROM business as b
								  GROUP BY postal_code
								  ORDER BY postal_code) as temp 
								  ON bn.postal_code = temp.postal_code
								  ORDER BY rank
);
SELECT * FROM popular_businesses
DROP INDEX bizz_postal_index;
DROP INDEX num_checkin_index;
CREATE INDEX bizz_postal_index on business (postal_code);
CREATE INDEX num_checkin_index on business (num_checkins);

-- review and avg_review diff
SELECT temp1.name, temp1.review_rating, temp3.num_checkins
FROM
	(SELECT bs.business_id, bs.name, bs.review_rating, bs.postal_code, (review_rating - avg_review) as review_diff
	FROM business as bs, (SELECT cast(avg(review_rating) as decimal(5,2)) as avg_review 
						  FROM BUSINESS as bsn) as temp0) as temp1

	INNER JOIN
		(SELECT business_id, name, coalesce(num_checkins,0) as num_checkins, 
					cast(avg_checkins as decimal(10,2)), cast((coalesce(num_checkins,0) - avg_checkins) as decimal(10,2)) as checkins_diff
		FROM business as bn
		INNER JOIN (SELECT postal_code, avg(num_checkins) as avg_checkins
					FROM business as b
					GROUP BY postal_code
					ORDER BY postal_code) as temp2 
		ON temp2.postal_code = bn.postal_code)as temp3
	ON temp3.business_id = temp1.business_id
WHERE review_diff > 0
AND postal_code = '89103'
ORDER BY review_diff desc, checkins_diff desc;

--
SELECT count(distinct business_id)
FROM business
WHERE postal_code = '89103';

SELECT cast(population as varchar(10))
FROM zipcodeData
WHERE zipcode = '89103';

SELECT meanIncome
FROM zipcodeData
WHERE zipcode = '89103';