//Student Name: PEIYU LIU
//Student ID: 31153291

// 1. A new UFO sighting records
//match conditions to get records, add these records info to new node
// Check database before change it.
// MATCH (u)-[:IN_YEAR_OF]->(y2:Year{year:2021}),
// (u)-[:IN_MONTH_OF]->(m2:Month{month:1}),
// (u)-[:IN_DAY_OF]->(d2:Day{day:14}),
// (u)-[:IN_HOUR_OF]->(h2:Hour{hour:23}),
// (u)-[:IS_HAPPENED_IN]->(l2:Location{state:'IN',city:'HIGHLAND',countyName:'LAKE'})
// RETURN u;


MATCH (u1:UFO)-[:IN_MONTH_OF]->(m:Month{month:8}),
(u1:UFO)-[:IN_DAY_OF]->(d:Day{day:14}),
(u1:UFO)-[:IN_HOUR_OF]->(h:Hour{hour:16}),
(u1:UFO)-[:IN_YEAR_OF]->(y:Year{year:1998}),
(u1:UFO)-[:IS_WEAHTER_CONDITION_OF]->(c1:Conds),
(u1:UFO)-[:IS_WEAHTER_DIRECTION_OF]->(w1:Wdire)
//RETURN INFORMATION
// create a new ufo node
CREATE (u:UFO{})
// set results into new node
SET u.duration='25 minutes'
SET u.text='Awesome lights were seen in the sky'
SET u.summary='Awesome lights'
SET u.pressure=u1.pressure
SET u.temp=u1.temp
SET u.windchill=u1.windchill
SET u.rain=u1.rain
SET u.vis=u1.vis
SET u.dewpt=u1.dewpt
SET u.thunder=u1.thunder
SET u.fog=u1.fog
SET u.precip=u1.precip
SET u.wspd=u1.wspd
SET u.tornado=u1.tornado
SET u.hum=u1.hum
SET u.snow=u1.snow
SET u.wgust=u1.wgust
SET u.heatindex=u1.heatindex
SET u.hail=u1.hail
// add existing relationships to new node
MERGE(u)-[:IN_YEAR_OF]->(y2:Year{year:2021})
MERGE (u)-[:IS_WEAHTER_CONDITION_OF]->(c1)
MERGE(u)-[:IS_WEAHTER_DIRECTION_OF]->(w1)
MERGE(u)-[:IN_MONTH_OF]->(m2:Month{month:1})
MERGE(u)-[:IN_DAY_OF]->(d2:Day{day:14})
MERGE(u)-[:IN_HOUR_OF]->(h2:Hour{hour:23})
MERGE(u)-[:IS_HAPPENED_IN]->(l2:Location{state:'IN',city:'HIGHLAND',countyName:'LAKE'})
RETURN u,l2,c1,w1,d2,m2,h2,y2;

//2. change unknow shape to ‘flying saucer’
//condition year [2011 and 2008]
// Conds:‘Clear’ to ‘Sunny/Clear’
MATCH (u:UFO)-[:IS_WEAHTER_CONDITION_OF]->(c:Conds{condition:toUpper('Clear')}),
(u:UFO)-[:IN_YEAR_OF]->(y:Year),
(u:UFO)-[:IS_SHAPE_OF]->(s:Shape{shape:toUpper('Unknown')})
WHERE y.year = 2011 or y.year = 2008
SET s.shape = 'flying saucer'
SET c.condition = 'Sunny/Clear'
RETURN u,y,s,c;

//3. delete city:'ARCADIA'
// condition: city:'ARCADIA', state:'FL'
// check MATCH (l:Location{city:'ARCADIA',state:'FL'}) RETURN l;
MATCH(l:Location{city:'ARCADIA',state:'FL'})
DETACH DELETE l;
