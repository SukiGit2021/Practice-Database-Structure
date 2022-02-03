// Name       :PEIYU LIU
// Unitcode   :FIT5137
// Student Id :31153291


//Tasks:

//1.	Create a new project in Neo4j and name it as TutorialWork5.

//2.	Add a new graph. Name the graph to MonUniGraph and set the password to graph.

//3.	After your graph has been created, start the graph then open Neo4j Browser.

//4.	Create the nodes for the students.
Merge (s1:Student {name:"Mirriam Tan",gender:"F",email:"mtan@student.monuni.com",age:20});
Merge (s2:Student {name:"Juan Murray",gender:"M",email:"jmur@student.monuni.com",age:18});
Merge (s3:Student {name:"Andy Lay",gender:"M",email:"alay@student.monuni.com",age:28});

//5.	Retrieve the nodes you recently created.
MATCH (n) RETURN n;

//6.	Create the nodes for the units.
Merge (u1:Unit {code:"IT001",title:"Database",credit:5});
Merge (u2:Unit {code:"IT002",title:"Java",credit:5});
Merge (u3:Unit {code:"IT003",title:"SAP",credit:10});
Merge (u4:Unit {code:"IT004",title:"Network",credit:5});

//7.	Create the relationship between students and units (hint: you need to use match to find the nodes before creating the relationship).
match (s1:Student)
match (u1:Unit)
where s1.name = "Mirriam Tan" and u1.code = "IT001"
create (s1) -[:ENROL]->(u1);

match (s1:Student)
match (u2:Unit)
where s1.name = "Mirriam Tan" and u2.code = "IT002"
create (s1) -[:ENROL]->(u2);

match (s2:Student)
match (u1:Unit)
where s2.name = "Juan Murray" and u1.code = "IT001"
create (s2) -[:ENROL]->(u1);

match (s2:Student)
match (u4:Unit)
where s2.name = "Juan Murray" and u4.code = "IT004"
create (s2) -[:ENROL]->(u4);

match (s3:Student)
match (u1:Unit)
where s3.name = "Andy Lay" and u1.code = "IT001"
create (s3) -[:ENROL]->(u1);

//As you have created your database, answer the following queries:

//1.	Display all the students’s email.
MATCH (s:Student)
RETURN s.email;

//2.	Find Database unit.
MATCH (u:Unit)
WHERE u.title = 'Database'
RETURN u; 

//3.	Show Andy’s age.
MATCH (s:Student)
WHERE s.name = 'Andy Lay'
RETURN s.age;

//4.	Find all students whose age range is in between 20-30.
MATCH (n:Student)
WHERE n.age >=20 and n.age<= 30 
RETURN n;

//5.	Update Java’s credit to 7.
MATCH (n:Unit)
WHERE n.title = 'Java'
    SET n.credit =7;