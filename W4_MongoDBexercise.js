/*
  Name       : PEIYU LIU
  Unitcode   : FIT 5137
  Student Id : 31153291
*/


//////////////////////////
//        CREATE        //
//////////////////////////

//1. Create a database called studentEnrolment.

use studentEnrolment
db
//   Create a collection called students.

db.createCollection("students")
show colelctions()
//2. Insert data into students collection.
db.students.insertMany([
  {
      "ID": 1001,
     "Firstname":"John",
      "Lastname":"Smith",
      "Address":
      {
          "StreetAddress":"123 Monash Drive",
          "Suburb":"Clayton",
          "State":"VIC",
          "Postcode":3168
      },
      "Gender":"Male",
      "Course":"MIT",
     "Year":2019,
      "Off-Campus":false,
      "Email Address":["jsmith@gmail.com", "jsmith@yahoo.com"]
  },
  {
      "ID": 1002,
     "Firstname":"Mary",
      "Lastname":"Citizen",
      "Address":
      {
          "StreetAddress":"900 Dandenong Road",
          "Suburb":"Caulfield East",
          "State":"VIC",
          "Postcode":3145
      },
      "Gender":"Female",
      "Course":"MDS",
     "Year":2018,
      "Off-Campus":true,
      "Email Address":"mcitizen@gmail.com"
  },
  {
      "ID": 1003,
     "Firstname":"Fred",
      "Lastname":"Bloggs",
      "Address":
      {
          "StreetAddress":"90 Wellington Road",
          "Suburb":"Clayton",
          "State":"VIC",
          "Postcode":3168,
      },
      "Gender":"Male",
      "Course":["MIT", "MBIS"],
     "Year":2017,
      "Off-Campus":false,
      "Email Address":"fredb@gmail.com"
  },
  {
      "ID": 1004,
     "Firstname":"Nick",
      "Lastname":"Nice",
      "Address":
      {
          "StreetAddress":"3 Robinson Avenue",
          "Suburb":"Kew",
          "State":"VIC",
          "Postcode":3080,
      },
      "Gender":"Male",
      "Course":"MCS",
     "Year":2018,
      "Off-Campus":false,
      "Email Address":"nicknice@gmail.com"
  },
  {
      "ID": 1005,
     "Firstname":"Wendy",
      "Lastname":"Wheat",
      "Address":
      {
          "StreetAddress":"6 Algorithm Street",
          "Suburb":"Malvern",
          "State":"VIC",
          "Postcode":3144,
      },
      "Gender":"Female",
      "Course":"GDS",
     "Year":2019,
      "Off-Campus":true,
      "Email Address":"wwheat@yahoo.com"
  }
])


//////////////////////////
//        READ         //
/////////////////////////

//1.	List all students in the students collection.
db.students.find().pretty()
//2.	Find student who are doing MIT course.
db.students.find({Course:{$elemMatch:{$eq:"MIT"}}})
//3.	Show all on-campus students.
db.students.find({"Off-Campus":{$eq:false}})
//4.	List all students who live in Clayton.
db.students.find({"Address.Suburb":{$eq:"Clayton"}})
//5.	List all male students who are enrolled after 2017.
db.students.find({"Year":{$gt:2017},"Gender":"Male"})
//6.	Show all students who live in Malvern or Caulfield.
db.students.find({$or:[{"Address.Suburb":{$eq:"Malvern"}},{"Address.Suburb":{$regex:/Caulfield/}}]})
//7.	Display the name of all students and their course.
db.students.aggregate([{$project:{"Firstname":1,"Lastname":1,"Course":1}}])
//8.	Show the name, course, and year enrolled for 2 students.
db.students.find({},{"Firstname":1,"Lastname":1,"Course":1,"Year":1}).limit(2)
//9.	Display all students and sort them by the enrolment year.
> db.students.find().sort({"Year":1})
//10.	Count how many students who are doing MBIS. (Hint: count())
db.students.find({"Course":"MBIS"}).count()
//11.	Count the number of students who are enrolled in between 2018 to 2019.
db.students.find({"Year":{$in:[2018,2019]}})
//12.	Find 3 students who are enrolled before 2019 and they are doing either MIT or MDS.
db.students.find({"Year":{$lt:2019}, "Course":{$in:["MIT","MDS"]}}).limit(2)
//13.	Sort the student list based on their year enrolled in descending order and display the name of the top 3 students.
db.students.find({},{"Firstname":1,"Lastname":1}).sort({"Year":-1}).limit(3)
//14.	Show students who are not doing MIT. Display only the names of these students and sort their names in ascending order.
db.students.find({"Course":{$nin:["MIT"]}},{"_id":0,"Firstname":1,"Lastname":1}).sort({"Firstname":1})
//15.	Find students who are doing double degree in both MIT and MBIS.
db.students.find({"Course":{$eq:["MIT","MBIS"]}})





//////////////////////////
//        UPDATE        //
//////////////////////////

//1.	Assuming Mary Citizen changes her course from MDS to MCS, update the collection to reflect this change.

db.students.updateOne({"Firstname":"Mary","Lastname":"Citizen"},{$set:{"Course":"MCS"}})
//2. Add Wendy Wheat’s birthday, which is on 30th October 1990.
db.students.updateOne({"Firstname":"Wendy","Lastname":"Wheat"},{$set:{"Birthday":"30-10-1990"}})

//3. If you stored email address for Nick Nice in a string, change it to an Array and add a new email address for Nick Nice: nnice@gmail.com.
db.students.updateOne({"Firstname":"Nick","Lastname":"Nice"},{$push:{"Email Address":["nicknice@gmail.com","nnice@gmail.com"]}})

//4. Add another email address for Nick Nice: nnice@monash.edu. (Hint: $push)
db.students.updateOne({"Firstname":"Nick","Lastname":"Nice"},{$push:{"Email Address":"nnice@monash.edu"}})

//5. Find students who are doing MIT course and add these units:
//   - Unit Code: FIT5137, Unit Name: Advanced Database Technology
//   - Unit Code: FIT9132, Unit Name: Introduction to Databases
//   You have to ensure that each unit is stored as an object.
db.students.update({"Course":"MIT"},{$set:{"Unit":[{"Unit Code":"FIT5137","Unit Name":"Advanced Database Technology"},{"Unit Code":"FIT9132","Unit Name":"Introduction to Databases"}]}})

//6. Remove the birthday field that we entered for Wendy Wheat. (Hint: $unset)
db.students.updateOne({"Firstname":"Wendy","Lastname":"Wheat"},{$unset:{"Birthday":1}})

//7. Replace the details of Fredd Bloggs to the following:
//   Student ID: 1003
//   First name: Fred
//   Last name: Bloggs
//   Gender: Male
//   Course:
//   1.	Course name: MIT, Year: 2017
//   2.	Course name: MBIS, Year: 2018
//   Off-campus: false
//   Email: fredb@gmail.com
{
  "ID": 1003,
 "Firstname":"Fred",
  "Lastname":"Bloggs",
  "Address":
  {
      "StreetAddress":"90 Wellington Road",
      "Suburb":"Clayton",
      "State":"VIC",
      "Postcode":3168,
  },
  "Gender":"Male",
  "Course":["MIT", "MBIS"],
 "Year":2017,
  "Off-Campus":false,
  "Email Address":"fredb@gmail.com"
}
db.students.update({"Firstname":"Fred","Lastname":"Bloggs"},{$set:{"Course":[{"Course name":"MIT","Year":2017},{"Course name":"MBIS","Year":2018}]}},{$unset:{"Year":1}})



//////////////////////////
//        DELETE        //
//////////////////////////

//1.	Delete one student who is enrolled as on-campus student.
db.students.remove({"Off-Campus":false},{justOne:true})
//2. Delete students who are enrolled before 2019 and are off-campus students.
db.students.remove({"Off-Campus": true,"Year":{$lt:2019}},{justOne:false})