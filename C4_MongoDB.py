# connect to mongodb,
# Reference: https://www.patricia-anong.com/blog/pycharm,https://www.mongodb.com/blog/post/getting-started-with-python-and-mongodb
_author = 'PEIYU LIU'
import datetime

from pymongo import MongoClient

client = MongoClient('localhost', 27017)
database = client['FIT5137MASL']
ufo = database['ufo']

# (i) combine the date data (from the fields for year, month, day and hour)
ufo.aggregate([
    {"$project": {"dateTime": {"$dateFromParts": {"year": "$year", "month": "$month", "day": "$day", "hour": "$hour"}},
                  "_id": 0}}]);
#
# (ii) store the combined date data in a new field called dateTime,
ufo.aggregate([
    {"$addFields": {
        "dateTime": {"$dateFromParts": {"year": "$year", "month": "$month", "day": "$day", "hour": "$hour"}}}},
    {"$merge": {"into": "ufo"}}
]);

# (iii) remove the fields for year, month, day and hour,
ufo.update_many({}, {"$unset": {"year": 1, "month": 1, "day": 1, "hour": 1}});

# (iv) add another field with your group name (e.g. groupName: "Group FIT5137" if your group name is FIT5137) and
ufo.update_many({}, {"$set": {"groupName": "Group Suki&Albee"}});

# (v) store the updated collection in a new collection called ufoDates.
ufo.aggregate([
    {"$match": {}},
    {"$out": "ufoDates"}
]);

# use new collection
ufoDates = database['ufoDates']

# C.1.4. Add the sighting with the following data to the ufoDates collection
# Reference: dataTime:https://www.analyticsvidhya.com/blog/2020/02/mongodb-in-python-tutorial-for-beginners-using-pymongo/
ufoDates.insert_one(
    {"state": "MA",
     "city": "BOSTON",
     "countryName": "SUFFOLK",
     "dateTime": datetime.datetime(1998, 7, 14, 23, 0),
     "shape": "sphere",
     "sightingObs":
         [{"duration": "40 min",
           "text": "I was going to my work on my night shift at the St Albin’s hospital and saw an unearthly ray of shooting lights which could be none other than a UFO!",
           "summary": "Unearthly ray of shooting lights"}]});

# C.1.5. Keeping the location, weather observations and dateTime information, find and remove the sighting observation record which has a duration of "2 1/2 minutes". To find the duration information please look closely at all of the fields of the documents in the ufoDates collection.
ufoDates.update_many({"sightingObs.duration": "2 1/2 minutes"}, {"$unset": {"sightingObs": ""}});

# C.1.6. Use the aggregation pipeline to answer the following queries:
# (i) What was the total number of sightings observed from the year 1990 to 2000 in the city of ‘SAN FRANCISCO’, in the state of ‘CA’. Your output can be in any format as long as it displays the required information.
# result1_6 = ufoDates.aggregate([
#     {"$match": {"city": "SAN FRANCISCO", "state": "CA",
#                 "dateTime": {"$gte":datetime(1990, 1, 1), "$lt": datetime.datetime(2001, 1, 1)}}},
#     {"$group": {"_id": "$state", "totalNumOfSObs": {"$sum": 1}}}
# ]);
#
# print(list(result1_6))

# (ii) Using one MongoDB Shell query, find the average temperature, humidity, pressure and rainfall observed for all fireball shaped UFO sightings. The output should also display the average values rounded to 3 decimal places.
result1_6_2 = ufoDates.aggregate([
    {"$match": {"shape": "fireball"}},
    {"$group": {"_id": "fireball",
                "avgTemp": {"$avg": "$weatherObs.temp"},
                "avgHumidity": {"$avg": "$weatherObs.hum"},
                "avgPressure": {"$avg": "$weatherObs.pressure"},
                "avgRainfallObs": {"$avg": "$weatherObs.rain"}}},
    {"$project": {"_id": "fireball",
                  "avgTemp": {"$round": ["$avgTemp", 3]},
                  "avgHumidity": {"$round": ["$avgHumidity", 3]},
                  "avgPressure": {"$round": ["$avgPressure", 3]},
                  "avgRainfallObs": {"$round": ["$avgRainfallObs", 3]}}}
]);
print(list(result1_6_2))
# (iii) What was the month with the highest UFO sightings?
result1_6_3 = ufoDates.aggregate([
    {"$project": {"monthName": {"$switch": {"branches": [
        {"case": {"$eq": [{"$month": "$dateTime"}, 1]}, "then": "January"},
        {"case": {"$eq": [{"$month": "$dateTime"}, 2]}, "then": "February"},
        {"case": {"$eq": [{"$month": "$dateTime"}, 3]}, "then": "March"},
        {"case": {"$eq": [{"$month": "$dateTime"}, 4]}, "then": "April"},
        {"case": {"$eq": [{"$month": "$dateTime"}, 5]}, "then": "May"},
        {"case": {"$eq": [{"$month": "$dateTime"}, 6]}, "then": "June"},
        {"case": {"$eq": [{"$month": "$dateTime"}, 7]}, "then": "July"},
        {"case": {"$eq": [{"$month": "$dateTime"}, 8]}, "then": "August"},
        {"case": {"$eq": [{"$month": "$dateTime"}, 9]}, "then": "September"},
        {"case": {"$eq": [{"$month": "$dateTime"}, 10]}, "then": "October"},
        {"case": {"$eq": [{"$month": "$dateTime"}, 11]}, "then": "November"},
        {"case": {"$eq": [{"$month": "$dateTime"}, 12]}, "then": "December"}],
        "default": "NO"}}}},
    {"$group": {"_id": "$monthName", "highest number of UFO sightings": {"$sum": 1}}},
    {"$sort": {"highest number of UFO sightings": -1}},
    {"$limit": 1}
]);
print(list(result1_6_3))
