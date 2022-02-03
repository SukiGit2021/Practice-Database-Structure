/////////////////
//  Keyspace  //
////////////////

//a.	Create a new keyspace called books_keyspace in one cluster with a total of 3 nodes.
CREATE KEYSPACE "books_keyspace" WITH replication = {'class':'SimpleStrategy','replication_factor':3};;
//b.	Switch to the newly created keyspace.
USE books_keyspace;
//c.	Alter the keyspace and update it to have a total node of 2 rather than 3.
ALTER KEYSPACE books_keyspace WITH replication ={'class': 'SimpleStrategy','replication_factor':2};

///////////////////////
// Create Operations //
///////////////////////

//a.	Create a new column family called books_by_author that consists of the author name, book’s publish year, book ID, book name, and rating.  Make the author name, publish year, and book ID as the primary key. You should use UUID for the book ID.
CREATE COLUMNFAMILY books_by_author(author_name text,publish_year INT ,book_name text,Rating double, book_id UUID, PRIMARY KEY(author_name, publish_year,book_id));
DROP TABLE books_keyspace.books_by_author;

//b.	Insert the following data to the column family:
/*
      Author Name       Publish Year	 Book Name	      Rating
      James Patterson	  2008	         Cross	          3
      Adam Liaw	        2016	         The Zen Kitchen	4.5
      Rob Galea	        2018	         Breakthrough	    4
*/
//Use UUID to create the book ID.
INSERT INTO books_by_author(author_name,publish_year,book_name,Rating, book_id) VALUES('James Patterson',2008,'Cross',3,UUID());
INSERT INTO books_by_author(author_name,publish_year,book_name,Rating, book_id) VALUES('Adam Liaw',2016,'The Zen Kitchen',4.5,UUID());
INSERT INTO books_by_author(author_name,publish_year,book_name,Rating, book_id) VALUES('Rob Galea',2018,'Breakthrough',4,UUID());

/////////////////////
// Read Operations //
/////////////////////

//a.	Find books that were written by James Patterson before 2009.
SELECT * FROM books_by_author WHERE author_name = 'James Patterson' and publish_year < 2009;
//b.	Find books with rating of 4.5.
//    Can you find any books? Write down your observation.

SELECT * FROM books_by_author WHERE Rating = 4.5;

InvalidRequest: Error from server: code=2200 [Invalid query] message="Cannot execute this query as it might involve data filtering and thus may have unpredictable performance. If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING"

/////////////////////
// Secondary Index //
/////////////////////

//a.	Create a secondary index for rating.
//    Can you find books with rating of 4.5 now?
CREATE INDEX Rating_index on books_keyspace.books_by_author(Rating);
SELECT * FROM books_by_author WHERE Rating = 4.5;

///////////////
// Timestamp //
///////////////

//a.	Show the timestamp of book name.
SELECT WRITETIME(book_name) FROM books_by_author;

//b.	Show the timestamp of author’s name.
//    Write down your observation.
SELECT WRITETIME(author_name) FROM books_by_author;
InvalidRequest: Error from server: code=2200 [Invalid query] message="Cannot use selection function writeTime on PRIMARY KEY part Author Name"

//c.	Add a new row with the following details:
/*
      Author’s name	: James Patterson
      Publish year	: 2017
      Book name		: Manning
      Rating		: 4.0
*/

INSERT INTO books_keyspace.books_by_author(author_name,publish_year,book_name,Rating, book_id) VALUES('James Patterson',2017,'Manning',4.0,UUID());
//d.	Find all books written by James Patterson.
SELECT * FROM books_by_author WHERE author_name = 'James Patterson';

///////////////////////
// Collections – Set //
///////////////////////

//a.	Create a new column for emails that can store a set of email values.
ALTER TABLE books_by_author ADD email set<text>;

//b.	Add James Patterson’s email address: james_patter@gmail.com. Can you update all records of James Patterson with a single update command?

UPDATE books_by_author set email = {'james_patter@gmail.com'} WHERE author_name = 'James Patterson';
InvalidRequest: Error from server: code=2200 [Invalid query] message="Some clustering keys are missing: publish_year, book_id"

//c.	If you cannot insert the email address from the previous question, insert the email address for each record of James Patterson.

UPDATE books_by_author set email = email + {'james_patter@gmail.com'} WHERE author_name = 'James Patterson' and publish_year = 2008 and book_id = d9c4c4e4-ca55-4ce3-b5df-ab2fc6472eab;
UPDATE books_by_author set email = email + {'james_patter@gmail.com'} WHERE author_name = 'James Patterson' and publish_year = 2017 and book_id = 0ae6e449-726c-4855-9a5e-1928115669d1;

//d.	Add an email address for Adam Liaw: adam_liaw@gmail.com.
UPDATE books_by_author set email = email + {'adam_liaw@gmail.com'} WHERE author_name = 'Adam Liaw' and publish_year = 2016 and book_id = 32822ef3-c6c6-4ca4-936b-82595e776331;

//e.	Add another email address of Adam Liaw: adamliaw@gmail.com.
UPDATE books_by_author set email = email + {'adamliaw@gmail.com'} WHERE author_name = 'Adam Liaw' and publish_year = 2016  and book_id = 32822ef3-c6c6-4ca4-936b-82595e776331;

//f.	Delete Adam Liaw’s email that we inserted first (adam_liaw@gmail.com).
UPDATE books_by_author set email = email - {'adam_liaw@gmail.com'} WHERE author_name = 'Adam Liaw' and publish_year = 2016  and book_id = 32822ef3-c6c6-4ca4-936b-82595e776331;

////////////////////////
// Collections – List //
////////////////////////

//a.	Add a new column called series to store the list of other book titles of the series.
ALTER TABLE books_by_author ADD series LIST<text>;

//b.	Add “Along Came a Spider” as one of the series of James Patterson’s book titled Cross.
UPDATE books_by_author set series = series + ['Along Came a Spider'] WHERE author_name = 'James Patterson' and publish_year = 2008 and book_id = d9c4c4e4-ca55-4ce3-b5df-ab2fc6472eab;

//c.	Add another series of James Patterson’s Cross, which is “Jack and Jill” and “Cat and Mouse”.
UPDATE books_by_author set series = series + ['Jack and Jill','Cat and Mouse'] WHERE author_name = 'James Patterson' and publish_year = 2008 and book_id = d9c4c4e4-ca55-4ce3-b5df-ab2fc6472eab;

//d.	Delete “Jack and Jill” from the list.
UPDATE books_by_author set series = series - ['Jack and Jill'] WHERE author_name = 'James Patterson' and publish_year = 2008 and book_id = d9c4c4e4-ca55-4ce3-b5df-ab2fc6472eab;


///////////////////////
// Collections – Map //
///////////////////////

//a.	Add a new column called ISBN and store it as a Map collection:
ALTER TABLE books_by_author ADD ISBN map<text,text>;

//b.	Add a new column to store this information for Adam Liaw’s The Zen Kitchen:
//    ISBN-10: 0733634311
//    ISBN-13: 978-0733634314

UPDATE books_by_author set ISBN = ISBN + {'ISBN-10':'0733634311', 'ISBN-13':'978-0733634314'} WHERE  author_name = 'Adam Liaw'  and publish_year = 2016 and book_id = 32822ef3-c6c6-4ca4-936b-82595e776331;

//c.	Delete the ISBN-13 from Adam Liaw’s The Zen Kitchen.
UPDATE books_by_author set ISBN = ISBN - {'ISBN-13'} WHERE  author_name = 'Adam Liaw'  and publish_year = 2016 and book_id = 32822ef3-c6c6-4ca4-936b-82595e776331;


//////////////////////////////
// User-Defined Types (UDT) //
//////////////////////////////
user-defiend type 

//a.	Create a UDT to store the product details. In this case, we want to store the number of pages, cover type (hardcover/softcover), book dimension length, width, height, and unit of measurement.
CREATE TYPE product_detials
(
pages INT,
cover_type text,
length double,
width double,
height double,
UOM text
);

//b.	Add this detail to Adam Liaw’s The Zen Kitchen:
/*
      Pages	: 240
      Cover type	: Hardcover
      Length	: 20.8
      Width	: 2.6
      Height 	: 25.6
      UOM	: cm
*/

ALTER TABLE books_by_author ADD book_detials frozen<product_detials>;

UPDATE books_by_author set book_detials = {pages: 240, cover_type: 'Hardcover', length:20.8, width:2.6,height:25.6,UOM:'cm'} WHERE author_name = 'Adam Liaw' and publish_year = 2016 and book_id = 32822ef3-c6c6-4ca4-936b-82595e776331;