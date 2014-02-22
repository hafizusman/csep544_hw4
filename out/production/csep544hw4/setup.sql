/*
Plan: Each plan has 
	a plan id (integer), 
	a name (say: "Basic", "Rental Plus", "Super Access" -- you can invent your own), 
	the maximum number of rentals allowed (e.g. "basic" allows one movie; "rental plus" allows three; "super access" allows five; again, these are your choices), and 
	the monthly fee. 
For this project, you are asked to insert four different rental plans. 
*/
CREATE TABLE PLANS(
	id integer PRIMARY KEY CLUSTERED,
	name varchar(15) NOT NULL,
	maxrentals integer NOT NULL,
	monthlyfee integer NOT NULL
	);
INSERT INTO PLANS VALUES (1, 'plan1', 1, 10);
INSERT INTO PLANS VALUES (2, 'plan2', 2, 20);
INSERT INTO PLANS VALUES (3, 'plan3', 3, 30);
INSERT INTO PLANS VALUES (4, 'plan4', 4, 40);
-- SELECT * FROM PLANS;


/*
Customer: a customer has 
	an id (integer), 
	a login, 
	a password, 
	a first name and 
	a last name. 
	Each customer has exactly one rental plan. 
*/
CREATE TABLE CUSTOMERS(
	id integer PRIMARY KEY CLUSTERED,
	login varchar(50) NOT NULL,
	password varchar(50) NOT NULL,
	fname varchar(50) NOT NULL,
	lname varchar(50),
	plan_id integer FOREIGN KEY REFERENCES PLANS(id)
	);
INSERT INTO CUSTOMERS VALUES (1, 'jd', 'j1o', 'joe', 'danger', 4);
INSERT INTO CUSTOMERS VALUES (2, 'ss', 's2s', 'sam', 'sung', 3);
INSERT INTO CUSTOMERS VALUES (3, 'ms', 'm3s', 'micro', 'soft', 2);
INSERT INTO CUSTOMERS VALUES (4, 'vw', 'v4w', 'volks', 'wagen', 1);
INSERT INTO CUSTOMERS VALUES (5, 'sh', 's5h', 'sea', 'hawks', 1);
INSERT INTO CUSTOMERS VALUES (6, 'id', 'i6d', 'in', 'dia', 2);
INSERT INTO CUSTOMERS VALUES (7, 'ff', 'f7f', 'french', 'fries', 3);
INSERT INTO CUSTOMERS VALUES (8, 'js', 'j8s', 'john', 'smith', 4);
-- SELECT * FROM CUSTOMERS;


/*
Rental: a "rental" entity represents the fact that a movie was rented by 
	a customer with a customer id. The movie is identified by 
	a movie id (from the IMDB database). The rental has 
	a status that can be open, or closed, and 
	the date and time the movie was checked out, to distinguish multiple rentals of the same movie by the same customer. 
When a customer first rents a movie, then you create an open entry in Rentals; when he returns it you update it to closed (you never delete it). Keeping the rental history helps you improve your business by doing data mining (but we don't do this in this class.) 
Each rental refers to exactly one customer. (It also refers to a single movie, but that's in a different database, so we don't model that as a relationship.) 
*/
CREATE TABLE RENTALS(
	customerid integer FOREIGN KEY REFERENCES CUSTOMERS(id),
	movieid	integer,
	status integer,
	date datetime2,
	CONSTRAINT chk_Status CHECK(status=0 OR status=1) -- opened = 1, closed = 0
	);
CREATE CLUSTERED INDEX RENTAL_Index ON RENTALS (customerid);

INSERT INTO RENTALS VALUES (1, 93055, 1, DATEADD(hour, 0, SYSDATETIME()));
INSERT INTO RENTALS VALUES (1, 349560, 1, DATEADD(hour, -1, SYSDATETIME()));
INSERT INTO RENTALS VALUES (1, 285769, 1, DATEADD(hour, -2, SYSDATETIME()));
INSERT INTO RENTALS VALUES (2, 272734, 1, DATEADD(hour, -3, SYSDATETIME()));
INSERT INTO RENTALS VALUES (2, 159622, 1, DATEADD(hour, -4, SYSDATETIME()));
INSERT INTO RENTALS VALUES (3, 596836, 1, DATEADD(hour, -5, SYSDATETIME()));
INSERT INTO RENTALS VALUES (3, 118685, 1, DATEADD(hour, -6, SYSDATETIME()));
INSERT INTO RENTALS VALUES (4, 497670, 1, DATEADD(hour, -7, SYSDATETIME()));
-- SELECT * FROM RENTALS;

/*
Some famous movies: 
	id;name;year
	93055;"Braveheart";1995
	349560;"Memento";2000
	285769;"Goodfellas";1990
	272734;"Gladiator";2000
	159622;"Chinatown";1974
	596836;"The Godfather";1972
	118685;"Forrest Gump";1994
	497670;"Schindler's List";1993
*/
