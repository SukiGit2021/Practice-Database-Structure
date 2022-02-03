/*
  Name       :
  Unitcode   :
  Student Id :
*/


//////////////////////////
//        Part 1        //
//////////////////////////

//1.	Show all people who live in Melbourne and whose nationality is AU.
db.person.aggregate([{ $match: { "location.ctiy": "melbourne", "nat": "AU" } }])

//2.	Display the first name, last name, city, and nationality for all people.
db.person.aggregate([{ $project: { "name.first": 1, "name.last": 1, "location.city": 1, "nat": 1, "_id": 0 } }])
//3.	Display all names (both first name and last name) in ascending order.
db.person.aggregate([{ $project: { "name.first": 1, "name.last": 1, "_id": 0 } }, { $sort: { "name.first": 1, "name.last": 1 } }]);
//4.	Show all people whose age are over 18 years old but less than 60 years old.
db.person.aggregate([{ $match: { "dob.age": { $gt: 18, $lt: 60 } } }]).pretty();
//5.	Show all people who live in Melbourne or Sydney.
db.person.aggregate([{ $match: { "location.city": { $in: ["melbourne", "sydney"] } } }]).pretty();
//6.	Show all people who live in Melbourne but whose nationality is not AU.
db.person.aggregate([{ $match: { "location.city": "melbourne", "nat": { $ne: "AU" } } }]).pretty();
//7.	Calculate the number of people based on the time zone. You can use the time zone description to group people together.
db.person.aggregate([
  {$group:{_id:"$location.timezone.description", numStudents:{$sum:1}}}
]);
//8.	Show the oldest person.
db.person.aggregate([
  {$sort:{"dob.age":-1}},{$limit:1}
]).pretty();
//9.	Count how many people live in Melbourne who are over 50 years old.
db.person.aggregate([
  {$match:{"location.city":"melbourne","dob.age":{$gt:50}}},{$group:{_id:"People", numPeople:{$sum:1}}}
]).pretty();

//10.	Find the minimum and maximum registered age.
db.person.aggregate([
{$unwind:"$registered"},
{$group:{_id:"registered age",min:{$min:"$registered.age"},max:{$max:"$registered.age"}}}
]).pretty();
//11.	Count how many people live in Melbourne but whose nationality is not AU.
db.person.aggregate([
  {$match:{"location.city":"melbourne","nat":{$ne:"AU"}}},{$group:{_id:"people", number:{$sum:1}}}
]).pretty();

//12.	Find the average age for each nationality and sort it from the highest to lowest value.
db.person.aggregate([
  {$group:{_id:{nationality:"$nat"},avgAge:{$avg:"$dob.age"}}},
  {$sort:{avgAge:-1}}
]).pretty();
//13.	Show all people who live in the time zone “+10:00” but they are not located in Tasmania, Victoria, or New South Wales.
db.person.aggregate([
{$match:{"location.timezone.offset":"+10:00","location.state":{$in:["tasmania","victoria","new south wales"]}}}
]).pretty();

//14.	List all states with time zone “+10:00”.

db.person.aggregate([
  {$match:{"location.timezone.offset":"+10:00"}},{$project:{"location.state":1,_id:0}}
]).pretty();
//15.	Find the total number of people for each gender in each nationality and sort the result based on the nationality in ascending order.

db.person.aggregate([
  {$group:{_id:{nationality:"$nat",gender:"$gender"}, numbers:{$sum:1}}},
  {$sort:{nationality:1}}
]).pretty();
//16.	Show the top 5 nationalities.
db.person.aggregate([
  {$group:{_id:"$nat", numPeople: {$sum:1}}},
  {$sort:{numPeople:-1}},
  {$limit:5}
]).pretty();

//17.	Using $concat, display all person names in this format: “mr carl jacobs”.

db.person.aggregate([
  {$project:{fullname:{$concat:["$name.title", " ", "$name.first", " ", "$name.last"]}, _id:0}}
]).pretty();
//18.	Show the name, email, and registration date of all people who registered after 2014.
db.person.aggregate([
  {$project:{registeredDate:{$convert:{input:"$registered.date", to:"date"}},fullname:{$concat:["$name.title", " ", "$name.first", " ", "$name.last"]},_id:0,"email":1,"registered.date":1}},
  {$match:{registeredDate:{$gt:new Date("2014")}}}
]).pretty();

//19.	Display all unique cities and sort it in ascending order. Make sure that there are no duplicates in your output.
db.person.aggregate([
  {$group:{_id:{city:"$location.city"}, number:{$sum:1}}},
  {$match:{number:1}},
  {$project:{city:1}},
  {$sort:{city:1}}
]);

//20.	Show the top 3 states with the most female population.
db.person.aggregate([
  {$match:{gender:"female"}},
  {$group:{_id:{state:"$location.state"}, number:{$sum:1}}},
  {$sort:{number:-1}},
  {$limit:3}
]);

//21.	List all unique titles in the contacts collection.
db.person.aggregate([
  {$group:{_id:{title:"$name.title"}, numbers:{$sum:1}}},
  {$sort:{number:-1}},
  {$project:{title:1}}
]);


//////////////////////////
//        Part 2        //
//////////////////////////

//1.	Find all universities in VIC.
db.universities.find({ "state": "VIC" }).pretty();
db.universities.aggregate([{ $match: { "state": "VIC" } }]).pretty();
//2.	Show the university name, state, and country for all universities.
db.universities.aggregate([{ $project: { "name": 1, "state": 1, "country": 1, "_id": 0 } }]).pretty();
//3.	Count how many universities in each state.
db.universities.aggregate([{ $group: { _id: "$state", "total_number": { $sum: 1 } } }]);
//4.	Using $out, carry the results of your aggregation in the previous question into a new collection called aggResults.
//    Show all collections in this database. Write down your observation.
db.universities.aggregate([{ $group: { _id: "$state", "total_number": { $sum: 1 } } }, { $out: "totaluni" }]);
show collections
db.totaluni.find().pretty();
//5.	Run the following:
db.universities.aggregate([
  { $match: { initial: "MonU" } },
  { $unwind: "$students" }
]).pretty()
//   What does $unwind do? Write down your observation.
//Split a certain array type field in the document into multiple pieces, 
//each containing one value in the array.

//6.	Calculate the total number of students for each university.
db.universities.aggregate([{ $unwind: "$students" }, { $group: { _id: "$name", numStudents: { $sum: "$students.number" } } }]).pretty();
//7.	Calculate the total students in VIC in 2018.
db.universities.aggregate([{ $unwind: "$students" }, { $match: { "state": "VIC", "students.year": 2018 } }, { $group: { _id: "$students.year", numStudents: { $sum: "$students.number" } } }]).pretty();

//8.	Using $lookup, do left outer join between universities and courses.
db.universities.aggregate([
  {
    $lookup: {
      from: "courses",
      localField: "initial",
      foreignField: "university",
      as: "uni_courses"
    }
  }
]).pretty()

//9.	Show the top 2 highest number of students, together with the corresponding year.
db.universities.aggregate([{ $unwind: "$students" }, { $sort: { "students.number": -1 } }, { $limit: 2 }, { $project: { "students.year": 1, "students.number": 1, _id: 0 } }]).pretty();

//10.	Using $addFields, add the foundation year of Monash University, which was on 1958, to your output.
db.universities.aggregate([{
  $addFields:{"foundationYear":1958}},{$match:{"name":"Monash University"}}]).pretty();

db.universities.update({},{$unset:{"foundationYear":""}});