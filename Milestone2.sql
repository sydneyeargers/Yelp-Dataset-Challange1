DROP TABLE BUSINESS CASCADE;
DROP TABLE ATTRIBUTES CASCADE;
DROP TABLE CATEGORY CASCADE;
DROP TABLE HOURS CASCADE;
DROP TABLE USERS CASCADE;
DROP TABLE ELITE CASCADE;
DROP TABLE FRIENDS CASCADE;
DROP TABLE CHECK_INS CASCADE;
DROP TABLE REVIEW CASCADE;


CREATE TABLE BUSINESS(
	business_id  VARCHAR(24) UNIQUE NOT NULL PRIMARY KEY,
	name VARCHAR(250),
	neighborhood VARCHAR(50),
	address VARCHAR(200),
	city VARCHAR(20),
	state CHAR(2),
	postal_code CHAR(10),
	latitude DECIMAL(10,7),
	longitude DECIMAL(10,7),
	stars DECIMAL(3,2),
	review_count INT,
	is_open BOOLEAN,
	num_checkins INT DEFAULT 0,
	review_rating DECIMAL(3,2) DEFAULT 0.0
);

CREATE TABLE ATTRIBUTES(
	business_id VARCHAR(24),
	attr_name VARCHAR(40),
	sub_attr VARCHAR(40),
	attr_value VARCHAR(40),
	CONSTRAINT pk_attributes PRIMARY KEY (business_id, attr_name, sub_attr), 
	CONSTRAINT fk_attributes_business FOREIGN KEY (business_id) REFERENCES BUSINESS(business_id)
);

CREATE TABLE CATEGORY(
	business_id  VARCHAR(24),
	category_name VARCHAR(40),
	CONSTRAINT pk_category PRIMARY KEY(business_id, category_name),
	CONSTRAINT fk_category_business FOREIGN KEY (business_id) REFERENCES BUSINESS(business_id)
);

CREATE TABLE HOURS(
	business_id VARCHAR(24),
	weekday VARCHAR(10),
	hours VARCHAR(11),
	CONSTRAINT pk_hours PRIMARY KEY (business_id, weekday, hours),
	CONSTRAINT fk_hours_business FOREIGN KEY (business_id) REFERENCES BUSINESS(business_id)
);

CREATE TABLE USERS (
	user_id VARCHAR(24) UNIQUE NOT NULL PRIMARY KEY,
	average_stars DECIMAL(3,2),
	yelping_since INT,
	user_name VARCHAR(50),
	review_count INT,
	useful INT,
	funny INT,
	cool INT,
	fans INT,
	compliment_cool INT,
	compliment_cute INT,
	compliment_funny INT,
	compliment_hot INT,
	compliment_list INT,
	compliment_more INT,
	compliment_note INT,
	compliment_photos INT,
	compliment_plain INT,
	compliment_profile INT,
	compliment_writer INT
);

CREATE TABLE ELITE(
	user_id VARCHAR(24),
	elite_year VARCHAR(4),
	CONSTRAINT pk_elite PRIMARY KEY (user_id, elite_year),
	CONSTRAINT fk_elite_user FOREIGN KEY (user_id) REFERENCES USERS (user_id)
);


CREATE TABLE FRIENDS(
	user_id VARCHAR(24),
	friend_id VARCHAR(24),
	CONSTRAINT pk_friends PRIMARY KEY (user_id, friend_id),
	CONSTRAINT fk_friends_user FOREIGN KEY (user_id) REFERENCES USERS (user_id)
);

CREATE TABLE CHECK_INS(
	business_id VARCHAR(24),
	time VARCHAR(11),
	weekday VARCHAR(10),
	checkin_no INT,
	CONSTRAINT pk_checkins PRIMARY KEY (business_id, time, weekday),
	CONSTRAINT fk_checkins_business FOREIGN KEY (business_id) REFERENCES BUSINESS(business_id)
);

CREATE TABLE REVIEW(
	review_id VARCHAR(24) UNIQUE NOT NULL PRIMARY KEY,
	user_id VARCHAR(24),
	business_id VARCHAR(24),
	stars CHAR(1),
	date INT,
	text VARCHAR(1000),
	useful INT,
	funny INT,
	cool INT,
	CONSTRAINT fk_review_users FOREIGN KEY (user_id) REFERENCES USERS(user_id),
	CONSTRAINT fk_review_business FOREIGN KEY (business_id) REFERENCES BUSINESS(business_id)
);

SELECT * FROM BUSINESS;
SELECT * FROM ATTRIBUTES;
SELECT * FROM CATEGORY;
SELECT * FROM HOURS;
SELECT * FROM USERS;
SELECT * FROM ELITE;
SELECT * FROM FRIENDS;
SELECT * FROM CHECK_INS;





