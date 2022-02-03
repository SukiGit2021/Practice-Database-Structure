//Student Name: PEIYU LIU
//Student ID: 31153291

//Queries
// CREATE INDEXES FOR efficient queries
// state, city and countyName are used frequently.
CREATE INDEX ON:Location(state);
CREATE INDEX ON:Location(city);
CREATE INDEX ON:Location(countyName);
// :schema check indexes

// 1. month = 4 UFO records
//Aprial = 4
MATCH(u:UFO)-[:IN_MONTH_OF]->(m:Month)
WHERE m.month =4
RETURN COUNT(u) AS UFO_records;
// ╒═════════════╕
// │"UFO_records"│
// ╞═════════════╡
// │102          │
// └─────────────┘

//2. unique weather conditions in lowercase letters.
// condition year<2014, state = 'AZ', shape = 'CIRCLE'
MATCH(u:UFO)-[:IN_YEAR_OF]->(y:Year),
(u:UFO)-[:IS_WEAHTER_CONDITION_OF]->(c:Conds),
(u:UFO)-[:IS_HAPPENED_IN]->(l:Location{state:'AZ'}),
(u:UFO)-[:IS_SHAPE_OF]->(s:Shape{shape:'CIRCLE'})
WHERE y.year<2014
RETURN DISTINCT toLower(c.condition) AS `unique weather conditions`;
// ╞══════════════════╡
// │"SCATTERED CLOUDS"│
// ├──────────────────┤
// │"CLEAR"           │
// ├──────────────────┤
// │"MOSTLY CLOUDY"   │
// ├──────────────────┤
// │"PARTLY CLOUDY"   │
// ├──────────────────┤
// │"OVERCAST"        │
// └──────────────────┘

//3. unique UFO shapes that appeared in 2015 but not in 2000
MATCH(u:UFO)-[:IN_YEAR_OF]->(y:Year{year:2015}),(u:UFO)-[:IS_SHAPE_OF]->(s:Shape)
WHERE NOT (u:UFO)-[:IN_YEAR_OF]->(y:Year{year:2000})
RETURN DISTINCT s.shape AS `unique shapes`;
// ╞═══════════╡
// │"SPHERE"   │
// ├───────────┤
// │"CIRCLE"   │
// ├───────────┤
// │"DISK"     │
// ├───────────┤
// │"LIGHT"    │
// ├───────────┤
// │"FORMATION"│
// ├───────────┤
// │"OVAL"     │
// ├───────────┤
// │"CONE"     │
// ├───────────┤
// │"FIREBALL" │
// ├───────────┤
// │"OTHER"    │
// ├───────────┤
// │"CHANGING" │
// ├───────────┤
// │"TRIANGLE" │
// └───────────┘

//4. unique years(ascending order)
// condition:‘at high speeds’ in the text
MATCH (u:UFO)-[:IN_YEAR_OF]->(y:Year)
WHERE u.text CONTAINS 'at high speeds'
RETURN DISTINCT y.year AS `year`
ORDER BY y.year ASC;
// ╞════════╡
// │2008    │
// ├────────┤
// │2011    │
// ├────────┤
// │2013    │
// └────────┘

//5. count each wind direction times.
// times of each direction in descending order.
MATCH(u:UFO)-[:IS_WEAHTER_DIRECTION_OF]->(wd:Wdire),
(u:UFO)-[:IN_YEAR_OF]->(y:Year)
RETURN wd.direction AS `wdire`,COUNT(wd.direction) AS `times of each direction`
ORDER BY COUNT(wd.direction) DESC;
// ╒══════════╤═════════════════════════╕
// │"wdire"   │"times of each direction"│
// ╞══════════╪═════════════════════════╡
// │"NORTH"   │745                      │
// ├──────────┼─────────────────────────┤
// │"SOUTH"   │299                      │
// ├──────────┼─────────────────────────┤
// │"WEST"    │217                      │
// ├──────────┼─────────────────────────┤
// │"SSE"     │184                      │
// ├──────────┼─────────────────────────┤
// │"SSW"     │183                      │
// ├──────────┼─────────────────────────┤
// │"SW"      │179                      │
// ├──────────┼─────────────────────────┤
// │"WSW"     │177                      │
// ├──────────┼─────────────────────────┤
// │"SE"      │172                      │
// ├──────────┼─────────────────────────┤
// │"WNW"     │138                      │
// ├──────────┼─────────────────────────┤
// │"ESE"     │132                      │
// ├──────────┼─────────────────────────┤
// │"NW"      │129                      │
// ├──────────┼─────────────────────────┤
// │"VARIABLE"│129                      │
// ├──────────┼─────────────────────────┤
// │"EAST"    │121                      │
// ├──────────┼─────────────────────────┤
// │"NNW"     │98                       │
// ├──────────┼─────────────────────────┤
// │"ENE"     │75                       │
// ├──────────┼─────────────────────────┤
// │"NE"      │71                       │
// ├──────────┼─────────────────────────┤
// │"NNE"     │70                       │
// └──────────┴─────────────────────────┘

//6. nearest city
// condition: city: ‘CORAL SPRINGS’; countyName:‘BROWARD’; state: ‘FL’.
MATCH (l:Location{city:'CORAL SPRINGS',countyName:'BROWARD',state:'FL'})
WITH l, point({longitude:l.lng,latitude:l.lat}) AS location1
MATCH(l2:Location)
WITH location1,l,l2,point({longitude:l2.lng,latitude:l2.lat}) AS location2
WHERE location1 <> location2
WITH l.city AS `source city`,l2.city AS `target city`,distance(location1,location2) AS distance
RETURN `source city`,`target city`,distance
ORDER BY distance ASC
LIMIT 1;
// ╒═══════════════╤═════════════╤════════════════╕
// │"source city"  │"target city"│"distance"      │
// ╞═══════════════╪═════════════╪════════════════╡
// │"CORAL SPRINGS"│"MARGATE"    │5394.95935153865│
// └───────────────┴─────────────┴────────────────┘

//7.year with the least number of different kinds of UFO shapes.
MATCH (u:UFO)-[:IN_YEAR_OF]->(y:Year),(u:UFO)-[:IS_SHAPE_OF]->(s:Shape)
RETURN y.year AS Year, COUNT(DISTINCT(s.shape)) AS `number of counting`
ORDER BY `number of counting` ASC
LIMIT 1;
// ╒══════╤════════════════════╕
// │"Year"│"number of counting"│
// ╞══════╪════════════════════╡
// │1997  │3                   │
// └──────┴────────────────────┘

//8.average temperature, pressure, and humidity
//condition:each UFO shape,average values rounded to 3 decimal places
MATCH (u:UFO)-[:IS_SHAPE_OF]->(s:Shape),(u:UFO)
WITH  DISTINCT(s.shape) AS `shape category`,round(AVG(u.temp),3) AS `average temperature`,round(AVG(u.hum),3) AS `average humidity`,round(AVG(u.pressure),3) AS `average pressure`
RETURN `shape category`,`average temperature`,`average humidity`,`average pressure`
ORDER BY `average humidity` DESC;

//9.top 3 counties with the most number of different cities.
MATCH (l:Location)
RETURN l.countyName AS countyName, COUNT(l.city) AS `city numbers`
ORDER BY `city numbers` DESC
LIMIT 3;
// ╒═════════════╤══════════════╕
// │"countyName" │"city numbers"│
// ╞═════════════╪══════════════╡
// │"WASHINGTON" │87            │
// ├─────────────┼──────────────┤
// │"JEFFERSON"  │79            │
// ├─────────────┼──────────────┤
// │"LOS ANGELES"│79            │
// └─────────────┴──────────────┘
//10.Rank the total number of UFO sighting
// condition:each state,descending order
MATCH(u:UFO)-[:IS_HAPPENED_IN]->(l:Location)
WITH l.state AS State
RETURN State, COUNT(*) AS `number of UFO sighting recordings`
ORDER BY State DESC;