USE graph;

-- a. What is the sum of all book prices? Give just the sum.
SELECT SUM(price_props.num_value) AS total_book_price
FROM node AS n
JOIN node_props AS price_props
  ON price_props.node_id = n.node_id
WHERE n.type = 'Book'
  AND price_props.propkey = 'price';

-- b. What people does Spencer know? Give just their names.
SELECT known_name.string_value AS name
FROM node_props AS spencer_name
JOIN edge AS knows_edge
  ON knows_edge.in_node = spencer_name.node_id
JOIN node AS known_person
  ON known_person.node_id = knows_edge.out_node
JOIN node_props AS known_name
  ON known_name.node_id = known_person.node_id
WHERE spencer_name.propkey = 'name'
  AND spencer_name.string_value = 'Spencer'
  AND knows_edge.type = 'knows'
  AND known_person.type = 'Person'
  AND known_name.propkey = 'name'
ORDER BY known_name.string_value;

-- c. What books did Spencer buy? Give title and price.
SELECT title_props.string_value AS title, price_props.num_value AS price
FROM node_props AS spencer_name
JOIN edge AS bought_edge
  ON bought_edge.in_node = spencer_name.node_id
JOIN node AS purchased_node
  ON purchased_node.node_id = bought_edge.out_node
JOIN node_props AS title_props
  ON title_props.node_id = purchased_node.node_id
JOIN node_props AS price_props
  ON price_props.node_id = purchased_node.node_id
WHERE spencer_name.propkey = 'name'
  AND spencer_name.string_value = 'Spencer'
  AND bought_edge.type = 'bought'
  AND purchased_node.type = 'Book'
  AND title_props.propkey = 'title'
  AND price_props.propkey = 'price'
ORDER BY title_props.string_value;

-- d. What people know each other? Give just a pair of names.
SELECT p1_name.string_value AS person_1, p2_name.string_value AS person_2
FROM edge AS e1
JOIN edge AS e2
  ON e2.in_node = e1.out_node
 AND e2.out_node = e1.in_node
 AND e2.type = 'knows'
JOIN node AS p1
  ON p1.node_id = e1.in_node
JOIN node AS p2
  ON p2.node_id = e1.out_node
JOIN node_props AS p1_name
  ON p1_name.node_id = p1.node_id
JOIN node_props AS p2_name
  ON p2_name.node_id = p2.node_id
WHERE e1.type = 'knows'
  AND p1.type = 'Person'
  AND p2.type = 'Person'
  AND p1_name.propkey = 'name'
  AND p2_name.propkey = 'name'
  AND e1.in_node < e1.out_node
ORDER BY person_1, person_2;

-- e. What books were purchased by people who Spencer knows?
-- Exclude books that Spencer already owns.
SELECT DISTINCT rec_title.string_value AS title, rec_price.num_value AS price
FROM node_props AS spencer_name
JOIN edge AS knows_edge
  ON knows_edge.in_node = spencer_name.node_id
 AND knows_edge.type = 'knows'
JOIN node AS friend_node
  ON friend_node.node_id = knows_edge.out_node
JOIN edge AS friend_bought
  ON friend_bought.in_node = friend_node.node_id
 AND friend_bought.type = 'bought'
JOIN node AS rec_book
  ON rec_book.node_id = friend_bought.out_node
JOIN node_props AS rec_title
  ON rec_title.node_id = rec_book.node_id
JOIN node_props AS rec_price
  ON rec_price.node_id = rec_book.node_id
WHERE spencer_name.propkey = 'name'
  AND spencer_name.string_value = 'Spencer'
  AND friend_node.type = 'Person'
  AND rec_book.type = 'Book'
  AND rec_title.propkey = 'title'
  AND rec_price.propkey = 'price'
  AND NOT EXISTS (
    SELECT 1
    FROM edge AS spencer_bought
    WHERE spencer_bought.in_node = spencer_name.node_id
      AND spencer_bought.out_node = rec_book.node_id
      AND spencer_bought.type = 'bought'
  )
ORDER BY title;

-- Expected output with the supplied sample data:
-- a. total_book_price = 253.45
-- b. Emily, Brendan
-- c. Cosmos 17.00
--    Database Design 195.00
-- d. Emily, Spencer
-- e. DNA and you 11.50
