// Name       :PEIYU LIU
// Unitcode   :FIT5137
// Student Id :31153291

//Tasks:
//Entities: Product,Customer,Category
//Relationship: Product-[:IS_IN]->Category, Customer-[:BOUGHT]->Product,Customer-[:ADD_WISHLIST]->Product,Customer-[:VIEWED{times:x}]->Product

// Create Custoemr
MERGE(c1:Customer{name:'Joe Baxton',age:25,email:'joeee_baxton@example.com'});
MERGE(c2:Customer{name:'Daniel Johnston',age:31,email:'dan_j@example.com'});
MERGE(c3:Customer{name:'Alex McGyver',age:22,email:'mcgalex@example.com'});
MERGE(c4:Customer{name:'Allison York',age:24,email:'ally_york1@example.com'});

//Category
CREATE (category1:Category {name:'Smartphones'});
CREATE (category2:Category {name:'Notebooks'});
CREATE (category3:Category {name:'Cameras'});
//Product 
CREATE (p:Products{name:"Sony Experia Z22",availability:true, Shippability:true,price:765.00});
CREATE (p:Products{name:"Samsung Galaxy S8",availability:true, Shippability:true,price:784.00});
CREATE (p:Products{name:"Sony Xperia XA1 Dual G3112",availability:false, Shippability:true,price:229.50});
CREATE (p:Products{name:"Apple iPhone 8 Plus 64GB",availability:false, Shippability:true,price:874.20});
CREATE (p:Products{name:"Xiaomi Mi Mix 2",availability:true, Shippability:true,price:420.87});
CREATE (p:Products{name:"Huawei P8 Lite",availability:true, Shippability:true,price:191.00}
);
CREATE (p:Products{name:"Acer Swift 3 SF314-51-34TX",availability:false, Shippability:true,price:595.00});
CREATE (p:Products{name:"HP ProBook 440 G4",availability:true, Shippability:true,price:771.30});
CREATE (p:Products{name:"Dell Inspiron 15 7577",availability:true, Shippability:true,price:1477.50});
CREATE (p:Products{name:"Nikon D7500 Kit 18-105mm VR",availability:true, Shippability:true,price:1612.35});
CREATE (p:Products{name:"Canon EOS 6D Mark II Body",availability:false, Shippability:true,price:1794.00});
CREATE (p:Products{name:"Apple MacBook A1534 12' Rose Gold",availability:true, Shippability:false,price:1293.00});


// Product-[:IS_IN]->Category, 
match (p1:Products)
match (c1:Category)
where p1.name = "Sony Experia Z22" and c1.name = "Smartphones"
create (p1) -[:IS_IN]->(c1);

match (p2:Products)
match (c2:Category)
where p2.name = "Samsung Galaxy S8" and c2.name = "Smartphones"
create (p2) -[:IS_IN]->(c2);

match (p3:Products)
match (c3:Category)
where p3.name = "Sony Xperia XA1 Dual G3112" and c3.name = "Smartphones"
create (p3) -[:IS_IN]->(c3);

match (p4:Products)
match (c4:Category)
where p4.name = "Apple iPhone 8 Plus 64GB" and c4.name = "Smartphones"
create (p4) -[:IS_IN]->(c4);

match (p5:Products)
match (c5:Category)
where p5.name = "Xiaomi Mi Mix 2" and c5.name = "Smartphones"
create (p5) -[:IS_IN]->(c5);

match (p6:Products)
match (c6:Category)
where p6.name = "Huawei P8 Lite" and c6.name = "Smartphones"
create (p6) -[:IS_IN]->(c6);

match (p7:Products)
match (c7:Category)
where p7.name = "Acer Swift 3 SF314-51-34TX" and c7.name = "Notebooks"
create (p7) -[:IS_IN]->(c7);

match (p8:Products)
match (c8:Category)
where p8.name = "HP ProBook 440 G4" and c8.name = "Notebooks"
create (p8) -[:IS_IN]->(c8);

match (p9:Products)
match (c9:Category)
where p9.name = "Dell Inspiron 15 7577" and c9.name = "Notebooks"
create (p9) -[:IS_IN]->(c9);

match (p10:Products)
match (c10:Category)
where p10.name = "Apple MacBook A1534 12' Rose Gold" and c10.name = "Notebooks"
create (p10) -[:IS_IN]->(c10);

match (p11:Products)
match (c11:Category)
where p11.name = "Nikon D7500 Kit 18-105mm VR" and c11.name = "Cameras"
create (p11) -[:IS_IN]->(c11);

match (p12:Products)
match (c12:Category)
where p12.name = "Canon EOS 6D Mark II Body" and c12.name = "Cameras"
create (p12) -[:IS_IN]->(c12);


//Customer-[:BOUGHT]->Product
match (p1:Products)
match (c1:Customer)
where p1.name = "Apple MacBook A1534 12' Rose Gold" and c1.name = "Joe Baxton"
create (p1) <-[:BOUGHT]-(c1);

match (p1:Products)
match (c1:Customer)
where p1.name = "Xiaomi Mi Mix 2" and c1.name = "Alex McGyver"
create (p1) <-[:BOUGHT]-(c1);

match (p1:Products)
match (c1:Customer)
where p1.name = "Huawei P8 Lite" and c1.name = "Allison York"
create (p1) <-[:BOUGHT]-(c1);

match (p1:Products)
match (c1:Customer)
where p1.name = "Sony Xperia XA1 Dual G3112" and c1.name = "Allison York"
create (p1) <-[:BOUGHT]-(c1);


//Customer-[:ADD_WISHLIST]->Product
match (p1:Products)
match (c1:Customer)
where p1.name = "Apple iPhone 8 Plus 64GB" and c1.name = "Joe Baxton"
create (p1) <-[:ADD_WISHLIST]-(c1);

match (p1:Products)
match (c1:Customer)
where p1.name = "Dell Inspiron 15 7577" and c1.name = "Daniel Johnston"
create (p1) <-[:ADD_WISHLIST]-(c1);

match (p1:Products)
match (c1:Customer)
where p1.name = "Sony Xperia XA1 Dual G3112" and c1.name = "Alex McGyver"
create (p1) <-[:ADD_WISHLIST]-(c1);

match (p1:Products)
match (c1:Customer)
where p1.name = "Nikon D7500 Kit 18-105mm VR" and c1.name = "Alex McGyver"
create (p1) <-[:ADD_WISHLIST]-(c1);

match (p1:Products)
match (c1:Customer)
where p1.name = "Acer Swift 3 SF314-51-34TX" and c1.name = "Allison York"
create (p1) <-[:ADD_WISHLIST]-(c1);

match (p1:Products)
match (c1:Customer)
where p1.name = "HP ProBook 440 G4" and c1.name = "Allison York"
create (p1) <-[:ADD_WISHLIST]-(c1);


//Customer-[:VIEWED{times:x}]->Product
match (p1:Products)
match (c1:Customer)
where p1.name = "Sony Experia Z22" and c1.name = "Joe Baxton"
create (p1) <-[:VIEWED{times:10}]-(c1);

match (p1:Products)
match (c1:Customer)
where p1.name = "Sony Experia Z22" and c1.name = "Daniel Johnston"
create (p1) <-[:VIEWED{times:10}]-(c1);

match (p1:Products)
match (c1:Customer)
where p1.name = "Dell Inspiron 15 7577" and c1.name = "Daniel Johnston"
create (p1) <-[:VIEWED{times:20}]-(c1);



// 1. Create a new project in Neo4j and name it as Tutorial9.
DONE
// 2. Add a new graph. Name the graph to eShopGraph and set the password to graph.
DONE
// 3. After your graph has been created, start the graph then open Neo4j Browser.
DONE
// 4. Create the nodes for Categories. For this exercise, you need to create a separate node for each product category for the purpose of maintaining the relationship between each product and category.
DONE
// 5. Retrieve the nodes you recently created.
DONE
// 6. Delete all nodes in the database.
DONE
// 7. Recreate the nodes for Categorise and also create the nodes for products. Include the relationships between products and categories when you create the nodes.

// 8. Search and retrieve the node you have recently created.

// 9. Create customer nodes.
DONE
// 10. Create the relationship for views (hint: you need to use match to find the nodes before creating the relationship).
DONE
// 11. Create the relationships for Wishlist.
DONE
// 12. Create the relationships for purchase.
DONE



// As you have created your database, answer the following queries:

// 1. Display all the customer’s email.
MATCH (c:Customer)
RETURN c.email;

// 2. Find customer whose name is Allison York.
MATCH (c:Customer)
WHERE c.name = "Allison York"
RETURN c;

// 3. Show Alex McGyver’s age.
MATCH (c:Customer)
WHERE c.name = "Alex McGyver"
RETURN c.age;

// 4. Find all customers whose age range is in between 25-30.
MATCH (c:Customer)
WHERE c.age <=30 and c.age >=25
RETURN c;

// 5. Display all products’ name and price.
MATCH (p:Products)
RETURN p.name,p.price;

// 6. Show all products with price over 1000.
MATCH (p:Products)
WHERE p.price >1000
RETURN p;

// 7. Show products that are not available.
MATCH (p:Products)
WHERE p.availability = false
RETURN p;

// 8. Show products that are available but not shippable.
MATCH (p:Products)
WHERE p.availability = true and p.Shippability = false
RETURN p;

// 9. Update Daniel Johnston’s node to include his nickname “Dan”.
MATCH (c:Customer)
WHERE c.name = 'Daniel Johnston'
SET c.nickname = 'Dan'
RETURN c;

// 10. Update Samsung Galaxy S8’s price to 650.
MATCH (p: Products)
WHERE p.name = 'Samsung Galaxy S8'
SET p.price = 650
RETURN p;