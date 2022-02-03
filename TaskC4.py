# Student Name: PEIYU LIU
# Student ID: 31153291

# download python neo4j driver
# python -m pip install py2neo

# upgrade neo4j to latest version
pip install py2neo --upgrade

# import library
import py2neo

# import functions from library
from py2neo import Graph,Node,Relationship

# connect to graph database
g= Graph(host = 'localhost',auth = ('neo4j','5137'))

# clear graph database
# g.run("MATCH (n) RETURN n")
# g.run("MATCH (n) DETACH DELETE n")

# import data-state
g.run("LOAD CSV WITH HEADERS FROM 'file:///states_a2.csv' AS data WITH data \
    WHERE data.city IS NOT NULL\
         MERGE(location:Location{city:toUpper(data.city),\
             lat:toFloat(data.lat),lng:toFloat(data.lng),\
                 countyName:toUpper(data.countyName),\
                 state:toUpper(data.state)});")
# import data-ufo
g.run("LOAD CSV WITH HEADERS FROM 'file:///ufo_a2.csv' AS data WITH data\
     CREATE(u:UFO{})\
        SET u.duration = (CASE data.`duration` WHEN '' THEN NULL WHEN 'NA' THEN NULL ELSE trim(data.`duration`)END)\
        SET u.text = (CASE data.`text` WHEN '' THEN NULL WHEN 'NA' THEN NULL ELSE trim(data.`text`)END)\
        SET u.summary = (CASE data.`summary`  WHEN '' THEN NULL WHEN 'NA' THEN NULL ELSE trim(data.`summary`)END)\
        SET u.pressure = (CASE toFloat(data.`pressure`) WHEN '' THEN NULL WHEN 'NA' THEN NULL ELSE toFloat(data.`pressure`)END)\
        SET u.temp = (CASE toFloat(data.`temp`) WHEN '' THEN NULL WHEN 'NA' THEN NULL ELSE toFloat(data.`temp`)END)\
        SET u.hail = (CASE toInteger(data.`hail`) WHEN '' THEN NULL WHEN 'NA' THEN NULL ELSE toInteger(data.`hail`)END)\
        SET u.heatindex = (CASE toFloat(data.`heatindex`) WHEN '' THEN NULL WHEN 'NA' THEN NULL ELSE toFloat(data.`heatindex`)END)\
        SET u.windchill =  (CASE data.`windchill` WHEN '' THEN NULL WHEN 'NA' THEN NULL ELSE toFloat(data.`windchill`)END)\
        SET u.rain = (CASE  data.`rain` WHEN '' THEN NULL WHEN 'NA' THEN NULL ELSE toInteger(data.`rain`)END)\
        SET u.vis = (CASE data.`vis` WHEN '' THEN NULL WHEN 'NA' THEN NULL ELSE toFloat(data.`vis`)END)\
        SET u.dewpt = (CASE data.`dewpt` WHEN '' THEN NULL WHEN 'NA' THEN NULL ELSE toFloat(data.`dewpt`)END)\
        SET u.thunder = (CASE data.`thunder` WHEN '' THEN NULL WHEN 'NA' THEN NULL ELSE toInteger(data.`thunder`)END)\
        SET u.fog = (CASE data.fog WHEN '' THEN NULL WHEN 'NA' THEN NULL ELSE toInteger(data.`fog`)END)\
        SET u.precip= (CASE data.precip WHEN '' THEN NULL WHEN 'NA' THEN NULL ELSE toFloat(data.`precip`)END)\
        SET u.wspd= (CASE data.wspd WHEN '' THEN NULL WHEN 'NA' THEN NULL ELSE toFloat(data.`wspd`)END)\
        SET u.tornado= (CASE data.tornado WHEN '' THEN NULL WHEN 'NA' THEN NULL ELSE toInteger(data.`tornado`)END)\
        SET u.hum= (CASE data.hum WHEN '' THEN NULL WHEN 'NA' THEN NULL ELSE toFloat(data.`hum`)END)\
        SET u.snow= (CASE data.snow WHEN '' THEN NULL WHEN 'NA' THEN NULL ELSE toInteger(data.`snow`)END)\
        SET u.wgust= (CASE data.wgust WHEN '' THEN NULL WHEN 'NA' THEN NULL ELSE toFloat(data.`wgust`) END)\
        SET u.city= (CASE data.city WHEN '' THEN NULL WHEN 'NA' THEN NULL ELSE toUpper(data.`city`)END)\
        SET u.state= (CASE data.state WHEN '' THEN NULL WHEN 'NA' THEN NULL ELSE toUpper(data.`state`) END)\
FOREACH(IGNOREME IN CASE data.shape WHEN '' THEN NULL WHEN 'NA' THEN NULL ELSE  toUpper(trim(data.shape)) END|\
    MERGE (s:Shape {shape:toUpper(trim(data.shape))})\
    MERGE (u) -[:IS_SHAPE_OF]-> (s)\
)\
FOREACH(IGNOREME IN CASE data.conds WHEN '' THEN NULL WHEN 'NA' THEN NULL ELSE  toUpper(trim(data.conds)) END|\
    MERGE (c:Conds {condition:toUpper(trim(data.conds))})\
    MERGE (u) -[:IS_WEAHTER_CONDITION_OF]-> (c)\
)\
FOREACH(IGNOREME IN CASE data.wdire WHEN '' THEN NULL WHEN 'NA' THEN NULL ELSE  toUpper(trim(data.wdire)) END|\
    MERGE (wd:Wdire {direction:toUpper(trim(data.wdire))})\
    MERGE (u) -[:IS_WEAHTER_DIRECTION_OF]-> (wd)\
)\
FOREACH(IGNOREME IN CASE data.month WHEN '' THEN NULL WHEN 'NA' THEN NULL ELSE data.month END|\
    MERGE (m:Month {month:data.month})\
    MERGE (u) -[:IN_MONTH_OF]-> (m)\
)\
FOREACH(IGNOREME IN CASE data.year WHEN '' THEN NULL WHEN 'NA' THEN NULL ELSE  data.year END|\
    MERGE (y:Year {year:data.year})\
    MERGE (u) -[:IN_YEAR_OF]-> (y)\
)\
FOREACH(IGNOREME IN CASE data.day WHEN '' THEN NULL WHEN 'NA' THEN NULL ELSE  data.day END|\
    MERGE (d:Day {day:data.day})\
    MERGE (u) -[:IN_DAY_OF]-> (d)\
)\
FOREACH(IGNOREME IN CASE data.hour WHEN '' THEN NULL WHEN 'NA' THEN NULL ELSE  data.hour END|\
    MERGE (h:Hour {hour:data.hour})\
    MERGE (u) -[:IN_HOUR_OF]-> (h));")

#create relationships
g.run("LOAD CSV WITH HEADERS FROM 'file:///ufo_a2.csv' AS data\
WITH data\
MATCH(u:UFO{\
    city:toUpper(data.city),\
    state:toUpper(data.state),\
    text:data.text,\
    summary:data.summary,\
    pressure:toFloat(data.pressure)})\
MATCH(location:Location{city:toUpper(data.city),state:toUpper(data.state)})\
CREATE(u)-[:IS_HAPPENED_IN]->(location);\
MATCH (y:Year) SET y.year = toInteger(y.year);\
MATCH (y:Month) SET y.month = toInteger(y.month);\
MATCH (y:Day) SET y.day = toInteger(y.day);\
MATCH (y:Hour) SET y.hour = toInteger(y.hour);")

# queries
g.run("CREATE INDEX ON:Location(state);\
CREATE INDEX ON:Location(city);\
CREATE INDEX ON:Location(countyName);");

# query 1
g.run("MATCH(u:UFO)-[:IN_MONTH_OF]->(m:Month)\
WHERE m.month =4\
RETURN COUNT(u) AS UFO_records;")

#query 2
g.run("MATCH(u:UFO)-[:IN_YEAR_OF]->(y:Year),\
(u:UFO)-[:IS_WEAHTER_CONDITION_OF]->(c:Conds),\
(u:UFO)-[:IS_HAPPENED_IN]->(l:Location{state:'AZ'}),\
(u:UFO)-[:IS_SHAPE_OF]->(s:Shape{shape:'CIRCLE'})\
WHERE y.year<2014\
RETURN DISTINCT toLower(c.condition) AS `unique weather conditions`;")

#query 3
g.run("MATCH(u:UFO)-[:IN_YEAR_OF]->(y:Year{year:2015}),(u:UFO)-[:IS_SHAPE_OF]->(s:Shape)\
WHERE NOT (u:UFO)-[:IN_YEAR_OF]->(y:Year{year:2000})\
RETURN DISTINCT s.shape AS `unique shapes`;")

#query 4 
g.run("MATCH (u:UFO)-[:IN_YEAR_OF]->(y:Year)\
WHERE u.text CONTAINS 'at high speeds'\
RETURN DISTINCT y.year AS `year`\
ORDER BY y.year ASC;")


# query 5 
g.run("MATCH(u:UFO)-[:IS_WEAHTER_DIRECTION_OF]->(wd:Wdire),\
(u:UFO)-[:IN_YEAR_OF]->(y:Year)\
RETURN wd.direction AS `wdire`,COUNT(wd.direction) AS `times of each direction`\
ORDER BY COUNT(wd.direction) DESC;")

# query 6
g.run("MATCH (l:Location{city:'CORAL SPRINGS',countyName:'BROWARD',state:'FL'}) \
WITH l, point({longitude:l.lng,latitude:l.lat}) AS location1 \
MATCH(l2:Location) \
WITH location1,l,l2,point({longitude:l2.lng,latitude:l2.lat}) AS location2 \
WHERE location1 <> location2 \
WITH l.city AS `source city`,l2.city AS `target city`,distance(location1,location2) AS distance \
RETURN `source city`,`target city`,distance \
ORDER BY distance ASC \
LIMIT 1;")

#query 7
g.run("MATCH (u:UFO)-[:IN_YEAR_OF]->(y:Year),(u:UFO)-[:IS_SHAPE_OF]->(s:Shape) \
RETURN y.year AS Year, COUNT(DISTINCT(s.shape)) AS `number of counting` \
ORDER BY `number of counting` ASC \
LIMIT 1;")


# query 8
g.run("MATCH (u:UFO)-[:IS_SHAPE_OF]->(s:Shape),(u:UFO)\
WITH  DISTINCT(s.shape) AS `shape category`,round(AVG(u.temp),3) AS `average temperature`,\
round(AVG(u.hum),3) AS `average humidity`,round(AVG(u.pressure),3) AS `average pressure`\
RETURN `shape category`,`average temperature`,`average humidity`,`average pressure`\
ORDER BY `average humidity` DESC;")


# query 9
g.run("MATCH (l:Location)\
RETURN l.countyName AS countyName, COUNT(l.city) AS `city numbers`\
ORDER BY `city numbers` DESC \
LIMIT 3;")

# query 10
g.run("MATCH(u:UFO)-[:IS_HAPPENED_IN]->(l:Location)\
WITH l.state AS State \
RETURN State, COUNT(*) AS `number of UFO sighting recordings`\
ORDER BY State DESC;")


# modification 1
g.run("MATCH (u1:UFO)-[:IN_MONTH_OF]->(m:Month{month:8}),\
(u1:UFO)-[:IN_DAY_OF]->(d:Day{day:14}),\
(u1:UFO)-[:IN_HOUR_OF]->(h:Hour{hour:16}),\
(u1:UFO)-[:IN_YEAR_OF]->(y:Year{year:1998}),\
(u1:UFO)-[:IS_WEAHTER_CONDITION_OF]->(c1:Conds),\
(u1:UFO)-[:IS_WEAHTER_DIRECTION_OF]->(w1:Wdire) \
CREATE (u:UFO{}) \
SET u.duration='25 minutes' \
SET u.text='Awesome lights were seen in the sky' \
SET u.summary='Awesome lights' \
SET u.pressure=u1.pressure \
SET u.temp=u1.temp \
SET u.windchill=u1.windchill \
SET u.rain=u1.rain \
SET u.vis=u1.vis \
SET u.dewpt=u1.dewpt \
SET u.thunder=u1.thunder \
SET u.fog=u1.fog \
SET u.precip=u1.precip \
SET u.wspd=u1.wspd \
SET u.tornado=u1.tornado \
SET u.hum=u1.hum \
SET u.snow=u1.snow \
SET u.wgust=u1.wgust \
SET u.heatindex=u1.heatindex \
SET u.hail=u1.hail \
MERGE(u)-[:IN_YEAR_OF]->(y2:Year{year:2021}) \
MERGE (u)-[:IS_WEAHTER_CONDITION_OF]->(c1) \
MERGE(u)-[:IS_WEAHTER_DIRECTION_OF]->(w1) \
MERGE(u)-[:IN_MONTH_OF]->(m2:Month{month:1}) \
MERGE(u)-[:IN_DAY_OF]->(d2:Day{day:14}) \
MERGE(u)-[:IN_HOUR_OF]->(h2:Hour{hour:23}) \
MERGE(u)-[:IS_HAPPENED_IN]->(l2:Location{state:'IN',city:'HIGHLAND',countyName:'LAKE'}) \
RETURN u,l2,c1,w1,d2,m2,h2,y2;")

# modification 2
g.run("MATCH (u:UFO)-[:IS_WEAHTER_CONDITION_OF]->(c:Conds{condition:toUpper('Clear')}),\
(u:UFO)-[:IN_YEAR_OF]->(y:Year),\
(u:UFO)-[:IS_SHAPE_OF]->(s:Shape{shape:toUpper('Unknown')}) \
WHERE y.year = 2011 or y.year = 2008 \
SET s.shape = 'flying saucer' \
SET c.condition = 'Sunny/Clear' \
RETURN u,y,s,c;")

# modification 3
g.run("MATCH(l:Location{city:'ARCADIA',state:'FL'}) \
DETACH DELETE l;")