USE datawarehouse;

CREATE TABLE uscounties_stage(
	county_fips varchar(5) NOT NULL,
	county varchar(50) NOT NULL,
	county_ascii varchar(50) NOT NULL,
	county_full varchar(50) NOT NULL,
	state_id varchar(5) NOT NULL,
	state_name varchar(50) NOT NULL,
	lat float NOT NULL,
	lng float NOT NULL,
	population int NOT NULL
);

CREATE TABLE state_aqi_stage(
	defining_site varchar(50) NOT NULL,
	date date NOT NULL,
	defining_parameter varchar(50) NOT NULL,
	state_code int NOT NULL,
	state_name varchar(50) NOT NULL,
	county_code int NOT NULL,
	county_name varchar(50) NOT NULL,
	aqi int NOT NULL,
	category varchar(50) NOT NULL,
	number_of_sites_reporting int NOT NULL,
	created datetime NOT NULL,
	last_updated datetime NOT NULL
);
