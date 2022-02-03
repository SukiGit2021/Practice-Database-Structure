/*start up cassandra shell and show keyspaces info*/
cassandra
cqlsh
DESCRIBE KEYSPACES;

--C.2.1
/* create a new keyspace and replication strategy is simplestrategy, replication factor is 1*/
CREATE KEYSPACE FIT5137_MASL WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};

--TRUNCATE ufos;

--C.2.2
/* create user defined type for sightingObs, it contains duration, text, summary*/
CREATE TYPE sightingObs_type
(
duration text,
text text,
summary text,
);

/*
Group cleaned original ufo.csv, combine each columns but split each columns' names and values by symbol |.
So when copy csv file to cassandra table, it can use delimiter = '|'.
create table ufos, all columns are matching with my cleaned ufo.csv. Use UDT for sightingObs.
Use state and month as parition keys, because one question need to group data by state and month.
there are many different years, days and hours, country names, cities, so I add these columns into composited key as a part of primary key.
Reference: https://docs.datastax.com/en/dse/5.1/cql/cql/cql_using/useInsertCopyCSV.html.
*/
CREATE TABLE ufos
(
    id text,
    state text,
    day int,
    month int,
    year int,
    hour int,
    shape text,
    city text,
    weatherObs_windchill double,
    weatherObs_wdire text,
    weatherObs_wspd double,
    weatherObs_pressure double,
    weatherObs_temp double,
    weatherObs_hail int,
    weatherObs_rain int,
    weatherObs_vis double,
    weatherObs_dewpt double,
    weatherObs_thunder int,
    weatherObs_fog int,
    weatherObs_tornado int,
    weatherObs_hum double,
    weatherObs_snow int,
    weatherObs_conds text,
    countyName text,
    sightingObs list<frozen<sightingObs_type>>,
    PRIMARY KEY((month,state),day,city,countyName,hour,year)
);

--Reference: https://docs.datastax.com/en/dse/5.1/cql/cql/cql_using/useInsertCopyCSV.html
COPY FIT5137_MASL.ufos(id,state,day,month,year,hour,shape,city,weatherObs_windchill,weatherObs_wdire,weatherObs_wspd,weatherObs_pressure,weatherObs_temp,weatherObs_hail,weatherObs_rain,weatherObs_vis,weatherObs_dewpt,weatherObs_thunder,weatherObs_fog,weatherObs_tornado,weatherObs_hum,weatherObs_snow,weatherObs_conds,countyName,sightingObs) 
FROM '/Users/lpylocal/Desktop/2021 semester 2/5137/assignment1/output/newufo.csv' WITH HEADER = TRUE AND DELIMITER='|';

--C.2.3
/*
(i)How many UFO sightings were recorded in the database for each month and each state?
two partition keys, month and state in my table, it can be deal with GROUP BY month and state. 
Select all UFO records, use COUNT to calculate numbers, and save the count result as a new name. 
Reference:https://stackoverflow.com/questions/53453405/cassandra-equivalent-of-group-by
*/
SELECT COUNT(*) AS numOfUfo,month,state FROM ufos GROUP BY month,state;

/*
(ii) How many UFO sightings were recorded during ‘Overcast’ weather conditions?
Condition is Overcast cond, select all records related to this condition and summarise numbers.
Reference: https://stackoverflow.com/questions/37114455/reading-error-in-cassandra
*/
--ALTER TABLE ufos WITH GC_GRACE_SECONDS = 0;
CREATE INDEX conds_index on FIT5137_MASL.ufos(weatherObs_conds);

SELECT COUNT(*) AS numOfUfo,weatherObs_conds  FROM ufos WHERE weatherObs_conds = 'Overcast' ALLOW FILTERING;


/*
(iii) What was the average temperature, pressure and humidity during the year 2000?
Condition is year =2000. Use avg() to calculate average temperature, pressure and humidity.
*/
--ALTER TABLE ufos WITH GC_GRACE_SECONDS = 0;
CREATE INDEX year_index on FIT5137_MASL.ufos(year);

SELECT year,AVG(weatherObs_temp) as averageTemperature, 
AVG(weatherObs_pressure) as averagePressure, 
AVG(weatherObs_hum) as averageHumidity 
FROM ufos where year = 2000 ALLOW FILTERING;

--C.2.4
/*
select details of weather record about that observed during 11th October 1998 10PM
insert new data
*/
SELECT weatherObs_windchill,weatherObs_wdire,weatherObs_wspd,weatherObs_pressure,weatherObs_temp,weatherObs_hail,
weatherObs_rain,weatherObs_vis,weatherObs_dewpt,weatherObs_thunder,weatherObs_fog,weatherObs_tornado,weatherObs_hum,
weatherObs_snow,weatherObs_conds FROM ufos WHERE day = 11 and month =10 and year = 1998 and hour = 22 ALLOW FILTERING;

INSERT INTO ufos (sightingObs,day,month,year,hour,city,countyName,state,weatherObs_wdire,
weatherObs_wspd,weatherObs_pressure,weatherObs_temp,weatherObs_hail,weatherObs_rain,weatherObs_vis,
weatherObs_dewpt,weatherObs_thunder,weatherObs_fog,weatherObs_tornado,weatherObs_hum,weatherObs_snow,weatherObs_conds)
VALUES([{duration:'25 minutes',text:'Awesome lights were seen in the sky',summary:'Awesome lights'}],14,1,2021,23,
'HIGHLAND','LAKE','IN','North',-0.911239,-0.372984,-0.20865,0,0,0.64272,-0.090929,0,0,0,0.02922,0,'Clear');


