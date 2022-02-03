// use FIT5137MASL collection
show dbs
use FIT5137MASL
show collections

//C.1 MongoDB shell
//(i) combine the date data (from the fields for year, month, day and hour) to proper MongoDB date data types.
// Use dataFromParts to concat year,month,day,hour Reference: https://docs.mongodb.com/manual/reference/operator/aggregation/dateFromParts/
db.ufo.aggregate([
{$project:{"dateTime":{$dateFromParts:{"year":"$year","month":"$month","day":"$day","hour":"$hour"}},_id:0}}
]);

//(ii) store the combined date data in a new field called dateTime.
// use addFields to add a new field then store combined date. Reference:https://docs.mongodb.com/manual/reference/operator/aggregation/addFields/
db.ufo.aggregate([
{$addFields:{"dateTime":{$dateFromParts:{"year":"$year","month":"$month","day":"$day","hour":"$hour"}}}},
{$merge:{into:"ufo"}}
]);

//(iii) remove the fields for year, month, day and hour.
// use unset to delete specified fields
db.ufo.updateMany({},{$unset:{"year":1,"month":1,"day":1,"hour":1}});

//(iv) add another field with your group name
//(e.g. groupName: "Group FIT5137" if your group name is FIT5137)
db.ufo.updateMany({},{$set:{"groupName":"Group Suki&Albee"}});


//(v) store the updated collection in a new collection called ufoDates.
// $out to output a new collection that stores changed ufo data.
db.ufo.aggregate([
{$match:{}},
{$out:"ufoDates"}
]);


//C.1.4. Add data to the ufoDates collection
// add new data into collection, dateTime use new Date(), sightingObs is a object contains other three string values.
db.ufoDates.insertOne(
{state:"MA",
city:"BOSTON",
countryName:"SUFFOLK",
dateTime:new Date("1998-07-14T23:00:00.000+00:00"),
shape:"sphere",
sightingObs:
[{duration:"40 min",
text:"I was going to my work on my night shift at the St Albin’s hospital and saw an unearthly ray of shooting lights which could be none other than a UFO!",
summary:"Unearthly ray of shooting lights"}]
});

//C.1.5.
// use update to modify data, unsure rows so use updateMany
// condition is duration time, delete sightingObs field after find matching data.
db.ufoDates.updateMany({"sightingObs.duration":"2 1/2 minutes"},{$unset:{"sightingObs":""}});

//C.1.6
//(i)
// conditions are state,city and time, group mathing data by state, then count numbers of data in state
db.ufoDates.aggregate([
{$match:{"city":"SAN FRANCISCO","state":"CA","dateTime":{$gte:ISODate("1990-01-01"),$lt:ISODate("2001-01-01")}}},
{$group:{_id:"$state", totalNumOfSObs:{$sum:1}}}
]);


//(ii)
//condition is fireball shape, group temp,hum,pre,rain to calculate average value and use $round to keep 3 decimal places for results
//Reference:https://docs.mongodb.com/manual/reference/operator/aggregation/round/
db.ufoDates.aggregate([
{$match:{"shape":"fireball"}},
{$group:{_id:"fireball",
avgTemp:{$avg:"$weatherObs.temp"},
avgHumidity:{$avg:"$weatherObs.hum"},
avgPressure:{$avg:"$weatherObs.pressure"},
avgRainfallObs:{$avg:"$weatherObs.rain"}}},
{$project:{_id:"fireball",
avgTemp:{$round:["$avgTemp",3]},
avgHumidity:{$round:["$avgHumidity",3]},
avgPressure:{$round:["$avgPressure",3]},
avgRainfallObs:{$round:["$avgRainfallObs",3]}}}
]);


//(iii)
// use $switch to change month numbers to month names. group by month and calculate numbers of each month, sort result as descending order and limit 1 output.
//Reference:https://docs.mongodb.com/manual/reference/operator/aggregation/switch/
db.ufoDates.aggregate([
{$project:{"monthName":{$switch:{branches:[
{case:{$eq:[{$month:"$dateTime"},1]},then:"January"},
{case:{$eq:[{$month:"$dateTime"},2]},then:"February"},
{case:{$eq:[{$month:"$dateTime"},3]},then:"March"},
{case:{$eq:[{$month:"$dateTime"},4]},then:"April"},
{case:{$eq:[{$month:"$dateTime"},5]},then:"May"},
{case:{$eq:[{$month:"$dateTime"},6]},then:"June"},
{case:{$eq:[{$month:"$dateTime"},7]},then:"July"},
{case:{$eq:[{$month:"$dateTime"},8]},then:"August"},
{case:{$eq:[{$month:"$dateTime"},9]},then:"September"},
{case:{$eq:[{$month:"$dateTime"},10]},then:"October"},
{case:{$eq:[{$month:"$dateTime"},11]},then:"November"},
{case:{$eq:[{$month:"$dateTime"},12]},then:"December"}],
default:"..."}}}},
{$group:{_id:"$monthName", "highest number of UFO sightings":{$sum:1}}},
{$sort:{"highest number of UFO sightings":-1}},
{$limit:1}
]);

//(iv)
// $max and $min function can pick maximum and minimum value from fields. filter null value colour and leave 3 decimal places.
// Reference:https://docs.mongodb.com/manual/reference/operator/aggregation/toUpper/
db.ufoDates.aggregate([
{$group:{_id:{$toUpper:"$colour"},maxTemp:{$max:"$weatherObs.temp"}, minTemp:{$min:"$weatherObs.temp"}}},
{$match:{_id:{$ne:""}}},
{$project:{_id:1,maxTemp:{$round:["$maxTemp",3]},minTemp:{$round:["$minTemp",3]}}}
]);


//(v)
// condition is oval shape, group by direction and summarise number, sort by descending order and pick 1 row.
db.ufoDates.aggregate([
{$match:{shape:"oval"}},
{$group:{_id:{Direction:"$weatherObs.windCond.wdire"},ObsRecord:{$sum:1}}},
{$sort:{ObsRecord:-1}},
{$limit:1}
]);


//(vi)
// create text index for text and summary, search contents which contain light or LIGHT.
//Reference:https://docs.mongodb.com/manual/reference/operator/query/text/
db.ufoDates.createIndex({"sightingObs.text":"text","sightingObs.summary":"text"});
db.ufoDates.aggregate([
{$match:{$text:{$search:"light", $caseSensitive: false}}},
{$count:"sumOfLight"}
]);

//C.1.7. lat,long:doubles
// city,state,countyname
// join two collections using $lookup. different state might have same city names,so use $let to generate new fields,
// use pipeline to aggregate multiple conditions. match state,city and county, then save location as one list
// $unwind to split new list field, save latitude and longitude as seperate field.
// output new collection
//Reference:https://docs.mongodb.com/manual/reference/operator/aggregation/lookup/
db.ufoDates.aggregate([
{$lookup:{from: "states",
let:{"post_city":"$city","post_countyName":"$countyName","post_state":"$state"},
pipeline:[
{$match:
{$expr:{$and:[
{$eq:["$$post_city","$city"]},
{$eq:["$$post_countyName","$countyName"]},
{$eq:["$$post_state","$state"]}]},
}},
{$project:{_id:0,countyName:0,city:0,state:0}}],
as:"geoDocs"}},
{$unwind:"$geoDocs"},
{$addFields:{"geoLat":"$geoDocs.lat","geoLng":"$geoDocs.lng"}},
{$project:{"geoDocs":0}},
{$out:"ufoStates"}
]);

//C.1.8 convert the latitude and longitude [coordinate:[long,lat]
// https://docs.mongodb.com/manual/reference/operator/aggregation/let/
// use let to defined new attribute
// https://docs.mongodb.com/manual/geospatial-queries/
// geo location
db.ufoStates.aggregate([
{$addFields:{location:{type:"Point", coordinates:["$geoLng","$geoLat"]}}},
{$project:{"geoLat":0,"geoLng":0}},
{$out:"ufoStatesGeojson"}
]);

//1.9
// find which city has most records, use location info of this city
// create index on coordinates[lng,lat]
// $geoNear to search points near my setting range[10000,100000]
// avoid same city names, so group by city and state
//https://blog.mlab.com/2014/08/a-primer-on-geospatial-data-and-mongodb/
//https://stackoverflow.com/questions/22374312/meteor-js-mongodb-near-geometry-geojson-point-coordinates-longitude-limit?rq=1
db.ufoStatesGeojson.aggregate([
{$group:{_id:"$city", sumOfCities:{$sum:1}}},
{$sort:{"sumOfCities":-1}},
{$limit:1}
]);
db.ufoStatesGeojson.find({city:{$eq:"PHOENIX"}},{"location.coordinates":1,"_id":0});
db.ufoStatesGeojson.createIndex({"location.coordinates":"2dsphere"});
db.ufoStatesGeojson.aggregate([
{$geoNear:{near: { type: "Point", coordinates: [-112.0891, 33.5722] },
        spherical:true,
        key:"location.coordinates",
        distanceField:"distance",
        minDistance:10000,
        maxDistance:100000}},
{$group:{_id:{"city":"$city","countyName":"$countyName","state":"$state"}}}]);

//1.10 output ufo.csv file
// compass action