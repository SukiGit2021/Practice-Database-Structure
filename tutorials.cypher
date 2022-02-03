//tutorial A
//load data
LOAD CSV WITH HEADERS FROM "file:///warehouse.csv" AS row 
WITH row 
WHERE row.warehouse_id IS NOT NULL 
MERGE (w:Warehouses {warehouse_id: row.warehouse_id}) 
ON CREATE SET w.location = row.location;

LOAD CSV WITH HEADERS FROM "file:///truck.csv" AS data
WITH data
WHERE data.truck_id IS NOT NULL
MERGE(tk:Truck{truck_id:data.truck_id})
ON CREATE SET tk.vol_capacity = data.vol_capacity,tk.weight_category=data.weight_category,tk.cost_per_km=data.cost_per_km;

LOAD CSV WITH HEADERS FROM "file:///trip.csv" AS data
WITH data
WHERE data.trip_id IS NOT NULL
MERGE(t:Trip{trip_id:data.trip_id})
ON CREATE SET t.trip_date = data.trip_date,t.total_km = data.total_km;

LOAD CSV WITH HEADERS FROM "file:///store.csv" AS data
WITH data
MERGE(s:Store{store_id:data.store_id})
ON CREATE SET s.store_name = data.store_name,s.store_address=data.store_address;



LOAD CSV WITH HEADERS FROM "file:///trip_from.csv" AS csvLine 
MATCH (t:Trip {trip_id: csvLine.trip_id}) 
MATCH (w:Warehouses {warehouse_id: csvLine.warehouse_id}) 
CREATE (t)-[:PICKUP_FROM]->(w);


LOAD CSV WITH HEADERS FROM "file:///trip.csv" AS data
WITH data
MATCH(t:Trip{trip_id:data.trip_id})
MATCH(tk:Truck{truck_id:data.truck_id})
CREATE (t)<-[:USED_TRIP]-(tk);

LOAD CSV WITH HEADERS FROM "file:///destination.csv" AS csvLine2
with csvLine2
MATCH (t:Trip {trip_id: csvLine2.trip_id})
MATCH (s:Store {store_id: csvLine2.store_id})
CREATE (t)-[:SEND_TO]->(s);

//query

//exercise B
MERGE (a:Location {name:'A'})
MERGE (b:Location {name:'B'})
MERGE (c:Location {name:'C'})
MERGE (d:Location {name:'D'})
MERGE (e:Location {name:'E'})
MERGE (f:Location {name:'F'}) 
MERGE (a)-[:ROAD {cost:50}]->(b)
MERGE (a)-[:ROAD {cost:50}]->(c)
MERGE (a)-[:ROAD {cost:100}]->(d)
MERGE (b)-[:ROAD {cost:40}]->(d)
MERGE (c)-[:ROAD {cost:40}]->(d)
MERGE (c)-[:ROAD {cost:80}]->(e)
MERGE (d)-[:ROAD {cost:30}]->(e)
MERGE (d)-[:ROAD {cost:80}]->(f)
MERGE (e)-[:ROAD {cost:40}]->(f)

CALL gds.graph.create(
	'myGraph',
	'Location',
	'ROAD',
	{
    	relationshipProperties: 'cost'
	}
)


// Step 8
MATCH (source:Location {name: 'A'}), (target:Location {name: 'F'})
CALL gds.shortestPath.dijkstra.stream('myGraph', {
	sourceNode: source,
	targetNode: target,
	relationshipWeightProperty: 'cost'
})
YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
RETURN
	index,
	gds.util.asNode(sourceNode).name AS sourceNodeName,
	gds.util.asNode(targetNode).name AS targetNodeName,
	totalCost,
	[nodeId IN nodeIds | gds.util.asNode(nodeId).name] AS nodeNames,
	costs,
	nodes(path) as path
ORDER BY index

// CALL db.schema.visualization() 
to check the LOOP design on the node

call db.schema.visualization()
