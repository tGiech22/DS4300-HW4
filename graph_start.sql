drop database if exists graph;
create database graph;
use graph;

create table node (
 node_id int primary key,
 type varchar(20)
 );
 
 create table edge (
 edge_id int primary key,
  in_node int,
  out_node int,
  type varchar(20)
  );
  
create table node_props (
  node_id int,
  propkey varchar(20),
  string_value varchar(100),
  num_value double
  );
  
 
 insert into node values 
 (1,'Person'),
 (2,'Person'),
 (3,'Person'),
 (4,'Person'),
 (5,'Person'),
 (6,'Book'),
 (7,'Book'),
 (8,'Book'),
 (9,'Book');
 
 insert into node_props values
 (1, 'name', 'Emily', null),
 (2, 'name', 'Spencer', null),
 (3, 'name', 'Brendan', null),
 (4, 'name', 'Trevor', null),
 (5, 'name', 'Paxton', null),
 (6, 'title', 'Cosmos', null),
 (6, 'price', null, 17.00),
 (7, 'title', 'Database Design', null),
 (7, 'price', null, 195.00),
 (8, 'title', 'The Life of Cronkite', null),
 (8, 'price', null, 29.95),
 (9, 'title', 'DNA and you', null),
 (9, 'price', null, 11.50);
 
 insert into edge values
 (1, 1, 7, 'bought'),
 (2, 2, 6, 'bought'),
 (3, 2, 7, 'bought'),
 (4, 3, 7, 'bought'),
 (5, 3, 9, 'bought'),
 (6, 4, 6, 'bought'),
 (7, 4, 7, 'bought'), 
 (8, 5, 7, 'bought'),
 (9, 5, 8, 'bought'),
 (10, 1,2,'knows'),
 (11, 2, 1, 'knows'),
 (12, 2, 3, 'knows');
 
 
-- a. what is the sum of all book prices?



-- b. how many people bought each book?
-- Give title and number of times purchased



-- c. What is the most expensive book?
-- Give title and price



-- d. Who does spencer know?



-- e. What books did spencer buy?  Give title and price.




-- f. Who knows each other?



-- g. What books were purchased by people who spencer knows?
-- You just have to combine two previous queries
-- Dropping price attribute in the process




