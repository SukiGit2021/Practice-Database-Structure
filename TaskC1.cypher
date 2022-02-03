//Student Name: PEIYU LIU
//Student ID: 31153291

// Task C1 Database Design.

// ● Create a new project called MASL.
//DONE
// ● Create a new graph called MASLgraph.
//DONE

// import csv to neo4j graph
//Analysis
//ufo: individual_nodes:[year,day,month,hour, shape,conds,wdire,location]
//state: state-countyname-city[latitude,longitude]
//Potential nodes: UFO,YEAR,DAY,MONTH,HOUR,SHAPE,WEATHER_CONDITION,WIND_DIRECTION,LOCATION(CITY,COUNTYNAME,STATE)
//Potential relationships: 
// (UFO)-[:IS_HAPPENED_IN]->(LOCATION)
// (UFO) -[:IS_SHAPE_OF]-> (SHAPE)
// (UFO) -[:IS_WEAHTER_CONDITION_OF]-> (WEAHTER_CONDITION)
// (UFO) -[:IS_WEAHTER_DIRECTION_OF]-> (WEAHTER_DIRECTION)
// (UFO) -[:IN_MONTH_OF]-> (MONTH)
// (UFO) -[:IN_YEAR_OF]-> (YEAR)
// (UFO) -[:IN_DAY_OF]-> (DAY)
// (UFO) -[:IN_HOUR_OF]-> (HOUR)

//Import state
LOAD CSV WITH HEADERS FROM 'file:///states_a2.csv' AS data
WITH data
// check null value
WHERE data.city IS NOT NULL
// set node with properties[city,county,state,lat,lng]
// ensure data format is right using toUpper and toFloat
MERGE(location:Location{
    city:toUpper(data.city),
    lat:toFloat(data.lat),
    lng:toFloat(data.lng), 
    countyName:toUpper(data.countyName),
    state:toUpper(data.state)});

//Import ufo
LOAD CSV WITH HEADERS FROM 'file:///ufo_a2.csv' AS data
WITH data
//Create nodes for each row in ufo.csv
CREATE(u:UFO{
})
// give conditions to check null value: CASE  X WHEN NULL THEN NULL ELSE X
// match data value with csv file, `` to avoid space between variables' name
 SET u.duration = (CASE data.`duration` WHEN "" THEN NULL WHEN "NA" THEN NULL ELSE trim(data.`duration`)END)
 // trim to remove empty space in front of string value or in the end of string value
 SET u.text = (CASE data.`text` WHEN "" THEN NULL WHEN "NA" THEN NULL ELSE trim(data.`text`)END)
 SET u.summary = (CASE data.`summary`  WHEN "" THEN NULL WHEN "NA" THEN NULL ELSE trim(data.`summary`)END)
 // ensure float and integer format when I set value to my properties
 SET u.pressure = (CASE toFloat(data.`pressure`) WHEN "" THEN NULL WHEN "NA" THEN NULL ELSE toFloat(data.`pressure`)END)
 SET u.temp = (CASE toFloat(data.`temp`) WHEN "" THEN NULL WHEN "NA" THEN NULL ELSE toFloat(data.`temp`)END)
 SET u.hail = (CASE toInteger(data.`hail`) WHEN "" THEN NULL WHEN "NA" THEN NULL ELSE toInteger(data.`hail`)END)
 SET u.heatindex = (CASE toFloat(data.`heatindex`) WHEN "" THEN NULL WHEN "NA" THEN NULL ELSE toFloat(data.`heatindex`)END)
 SET u.windchill =  (CASE data.`windchill` WHEN "" THEN NULL WHEN "NA" THEN NULL ELSE toFloat(data.`windchill`)END)
 SET u.rain = (CASE  data.`rain` WHEN "" THEN NULL WHEN "NA" THEN NULL ELSE toInteger(data.`rain`)END)
 SET u.vis = (CASE data.`vis` WHEN "" THEN NULL WHEN "NA" THEN NULL ELSE toFloat(data.`vis`)END)
 SET u.dewpt = (CASE data.`dewpt` WHEN "" THEN NULL WHEN "NA" THEN NULL ELSE toFloat(data.`dewpt`)END)
 SET u.thunder = (CASE data.`thunder` WHEN "" THEN NULL WHEN "NA" THEN NULL ELSE toInteger(data.`thunder`)END)
 SET u.fog = (CASE data.fog WHEN "" THEN NULL WHEN "NA" THEN NULL ELSE toInteger(data.`fog`)END)
 SET u.precip= (CASE data.precip WHEN "" THEN NULL WHEN "NA" THEN NULL ELSE toFloat(data.`precip`)END)
 SET u.wspd= (CASE data.wspd WHEN "" THEN NULL WHEN "NA" THEN NULL ELSE toFloat(data.`wspd`)END)
 SET u.tornado= (CASE data.tornado WHEN "" THEN NULL WHEN "NA" THEN NULL ELSE toInteger(data.`tornado`)END)
 SET u.hum= (CASE data.hum WHEN "" THEN NULL WHEN "NA" THEN NULL ELSE toFloat(data.`hum`)END)
 SET u.snow= (CASE data.snow WHEN "" THEN NULL WHEN "NA" THEN NULL ELSE toInteger(data.`snow`)END)
 SET u.wgust= (CASE data.wgust WHEN "" THEN NULL WHEN "NA" THEN NULL ELSE toFloat(data.`wgust`) END)
 SET u.city= (CASE data.city WHEN "" THEN NULL WHEN "NA" THEN NULL ELSE toUpper(data.`city`)END)
 SET u.state= (CASE data.state WHEN "" THEN NULL WHEN "NA" THEN NULL ELSE toUpper(data.`state`) END)
// traverse each row and unwind shape as individual node for query's convenience.
// CASE WHEN ELSE END  to deal with null value.
// Create relationships with UFO
FOREACH(IGNOREME IN CASE data.shape WHEN NULL THEN NULL WHEN "NA" THEN NULL ELSE  toUpper(trim(data.shape)) END|
    MERGE (s:Shape {shape:toUpper(trim(data.shape))})
    MERGE (u) -[:IS_SHAPE_OF]-> (s)
)
// traverse each row and unwind weahter condition as individual node for query's convenience.
// CASE WHEN ELSE END  to deal with null value.
// Create relationships with UFO
FOREACH(IGNOREME IN CASE data.conds WHEN NULL THEN NULL WHEN "NA" THEN NULL ELSE  toUpper(trim(data.conds)) END|
    MERGE (c:Conds {condition:toUpper(trim(data.conds))})
    MERGE (u) -[:IS_WEAHTER_CONDITION_OF]-> (c)
)
// traverse each row and unwind weather direction as individual node for query's convenience.
// CASE WHEN ELSE END  to deal with null value.
// Create relationships with UFO
FOREACH(IGNOREME IN CASE data.wdire WHEN NULL THEN NULL WHEN "NA" THEN NULL ELSE  toUpper(trim(data.wdire)) END|
    MERGE (wd:Wdire {direction:toUpper(trim(data.wdire))})
    MERGE (u) -[:IS_WEAHTER_DIRECTION_OF]-> (wd)
)
// traverse each row and unwind month as individual node for query's convenience.
// CASE WHEN ELSE END  to deal with null value.
// Create relationships with UFO
FOREACH(IGNOREME IN CASE data.month WHEN NULL THEN NULL WHEN "NA" THEN NULL ELSE data.month END|
    MERGE (m:Month {month:data.month})
    MERGE (u) -[:IN_MONTH_OF]-> (m)
)
// traverse each row and unwind year as individual node for query's convenience.
// CASE WHEN ELSE END  to deal with null value.
// Create relationships with UFO
FOREACH(IGNOREME IN CASE data.year WHEN NULL THEN NULL WHEN "NA" THEN NULL ELSE  data.year END|
    MERGE (y:Year {year:data.year})
    MERGE (u) -[:IN_YEAR_OF]-> (y)
)
// traverse each row and unwind day as individual node for query's convenience.
// CASE WHEN ELSE END  to deal with null value.
// Create relationships with UFO
FOREACH(IGNOREME IN CASE data.day WHEN NULL THEN NULL WHEN "NA" THEN NULL ELSE  data.day END|
    MERGE (d:Day {day:data.day})
    MERGE (u) -[:IN_DAY_OF]-> (d)
)
// traverse each row and unwind hour as individual node for query's convenience.
// CASE WHEN ELSE END  to deal with null value.
// Create relationships with UFO
FOREACH(IGNOREME IN CASE data.hour WHEN NULL THEN NULL WHEN "NA" THEN NULL ELSE  data.hour END|
    MERGE (h:Hour {hour:data.hour})
    MERGE (u) -[:IN_HOUR_OF]-> (h)
);

//Create relationships between UFO and where was it happened.
LOAD CSV WITH HEADERS FROM 'file:///ufo_a2.csv' AS data
WITH data
// MATCH right UFO and Location nodes.
MATCH(u:UFO{
    city:toUpper(data.city),
    state:toUpper(data.state),
    text:data.text,
    summary:data.summary,
    pressure:toFloat(data.pressure)})
MATCH(location:Location{city:toUpper(data.city),state:toUpper(data.state)})
// create relationship
CREATE(u)-[:IS_HAPPENED_IN]->(location);
// change year,month,hour,day to int format.
MATCH (y:Year) SET y.year = toInteger(y.year);
MATCH (y:Month) SET y.month = toInteger(y.month);
MATCH (y:Day) SET y.day = toInteger(y.day);
MATCH (y:Hour) SET y.hour = toInteger(y.hour);