import java.util.Properties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import java.io.FileInputStream;

/**
 * Runs queries against a back-end database
 */
public class Query {
    private Integer TEST = 1;
    private String configFilename;
    private Properties configProps = new Properties();

    private String jSQLDriver;
    private String jSQLUrl;
    private String jSQLUser;
    private String jSQLPassword;

    private static final Integer RENTAL_STATUS_OPENED = 1;
    private static final Integer RENTAL_STATUS_CLOSED = 0;

    // DB Connection
    private Connection conn;
    private Connection customerConn;

    // Canned queries

    private static final String SEARCH_SQL =
            "SELECT * FROM movie WHERE name LIKE ? ORDER BY id";
    private PreparedStatement searchStatement;

    private static final String DIRECTOR_MID_SQL = "SELECT y.* "
            + "FROM movie_directors x, directors y "
            + "WHERE x.mid = ? and x.did = y.id";
    private PreparedStatement directorMidStatement;

    private static final String ACTOR_MID_SQL =
            "SELECT A.fname, A.lname "
            + "FROM CASTS AS C "
            + "INNER JOIN MOVIE M ON M.id = C.mid "
            + "INNER JOIN ACTOR A ON A.id = C.pid "
            + "WHERE M.id = ? "
            + "GROUP BY A.id, A.fname, A.lname";
    private PreparedStatement actorMidStatement;

    private static final String CUSTOMER_LOGIN_SQL =
            "SELECT * FROM CUSTOMERS WHERE login = ? and password = ?";
    private PreparedStatement customerLoginStatement;

    private static final String REMAINING_RENTALS_SQL =
            "SELECT "
            + "(SELECT P.maxrentals FROM CUSTOMERS AS C INNER JOIN PLANS AS P ON P.id=C.plan_id WHERE C.id=? GROUP BY P.maxrentals) "
            + "- "
            + "(SELECT COUNT(*) FROM CUSTOMERS AS C INNER JOIN PLANS AS P ON P.id=C.plan_id INNER JOIN RENTALS AS R ON R.customerid=C.id WHERE C.id=? AND R.status=" + RENTAL_STATUS_OPENED + ")";
    private PreparedStatement remainingRentalsStatement;

    private static final String CUSTOMER_NAME_SQL =
            "SELECT C.fname, C.lname FROM CUSTOMERS AS C WHERE C.id=?";
    private PreparedStatement customerNameStatement;

    private static final String IS_VALID_MOVIE_ID_SQL =
            "SELECT COUNT(*) FROM MOVIE AS M WHERE M.id=?";
    private PreparedStatement isValidMovieIdStatement;

    private static final String IS_VALID_PLAN_ID_SQL =
            "SELECT COUNT(*) FROM PLANS AS P WHERE P.id=?";
    private PreparedStatement isValidPlanIdStatement;

    private static final String CUSTOMER_ID_FROM_RENTAL_SQL =
            "SELECT R.customerid FROM RENTALS AS R WHERE R.movieid=? AND R.status = " + RENTAL_STATUS_OPENED;
    private PreparedStatement customerIdFromRentalStatement;

    private static final String FAST_SEARCH_SQL =
            "SELECT * FROM MOVIE AS M WHERE LOWER(M.name) LIKE ? ORDER BY M.id";
    private PreparedStatement fastSearchStatement;

    private static final String FAST_SEARCH_DIRECTORS_SQL =
            "SELECT X.id AS mid, X.name AS mname, X.year AS myear, D.* " +
            "FROM MOVIE_DIRECTORS AS MD " +
            "INNER JOIN DIRECTORS AS D ON MD.did = D.id " +
            "RIGHT OUTER JOIN ( " +
                    "SELECT * " +
                    "FROM MOVIE AS M " +
                    "WHERE LOWER(M.name) LIKE ?) AS X " +
                    "ON X.id = MD.mid " +
                    "ORDER BY X.id";
    private PreparedStatement fastSearchDirectorsStatement;

    private static final String FAST_SEARCH_ACTORS_SQL =
            "SELECT X.id AS mid, A.id, A.fname, A.lname " +
            "FROM CASTS AS C " +
            "INNER JOIN ACTOR A ON A.id = C.pid " +
            "RIGHT OUTER JOIN ( " +
                "SELECT * " +
                "FROM MOVIE AS M " +
                "WHERE LOWER(M.name) LIKE ?) AS X " +
            "ON X.id = C.mid " +
            "GROUP BY X.id, A.id, A.fname, A.lname " +
            "ORDER BY X.id";
    private PreparedStatement fastSearchActorsStatement;

	/*
	private static final String BEGIN_TRANSACTION_SQL =
		"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION;";
	private PreparedStatement beginTransactionStatement;

	private static final String COMMIT_SQL = "COMMIT TRANSACTION";
	private PreparedStatement commitTransactionStatement;

	private static final String ROLLBACK_SQL = "ROLLBACK TRANSACTION";
	private PreparedStatement rollbackTransactionStatement;
	*/


    public Query(String configFilename) {
        this.configFilename = configFilename;
    }

    /**********************************************************/
    /* Connection code to SQL Azure. Example code below will connect to the imdb database on Azure
       IMPORTANT NOTE:  You will need to create (and connect to) your new customer database before 
       uncommenting and running the query statements in this file .
     */

    public void openConnection() throws Exception {
        configProps.load(new FileInputStream(configFilename));

        jSQLDriver   = configProps.getProperty("videostore.jdbc_driver");
        jSQLUrl	   = configProps.getProperty("videostore.imdb_url");
        jSQLUser	   = configProps.getProperty("videostore.sqlazure_username");
        jSQLPassword = configProps.getProperty("videostore.sqlazure_password");


		/* load jdbc drivers */
        Class.forName(jSQLDriver).newInstance();

		/* open connections to the imdb database */

        conn = DriverManager.getConnection(jSQLUrl, // database
                jSQLUser, // user
                jSQLPassword); // password

        conn.setAutoCommit(true); //by default automatically commit after each statement

		/* You will also want to appropriately set the 
                   transaction's isolation level through:  
		   conn.setTransactionIsolation(...) */

		/* Also you will put code here to specify the connection to your
		   customer DB.  E.g.

		   customerConn = DriverManager.getConnection(...);
		   customerConn.setAutoCommit(true); //by default automatically commit after each statement
		   customerConn.setTransactionIsolation(...); //
		*/
        jSQLUrl	   = configProps.getProperty("videostore.customer_url");
        customerConn = DriverManager.getConnection(jSQLUrl, // database
                jSQLUser, // user
                jSQLPassword); // password
    }

    public void closeConnection() throws Exception {
        conn.close();
        customerConn.close();
    }

    /**********************************************************/
    /* prepare all the SQL statements in this method.
      "preparing" a statement is almost like compiling it.  Note
       that the parameters (with ?) are still not filled in */

    public void prepareStatements() throws Exception {

        searchStatement = conn.prepareStatement(SEARCH_SQL);
        directorMidStatement = conn.prepareStatement(DIRECTOR_MID_SQL);
        fastSearchStatement = conn.prepareStatement(FAST_SEARCH_SQL);
        fastSearchDirectorsStatement = conn.prepareStatement(FAST_SEARCH_DIRECTORS_SQL);
        fastSearchActorsStatement = conn.prepareStatement(FAST_SEARCH_ACTORS_SQL);
        customerLoginStatement = customerConn.prepareStatement(CUSTOMER_LOGIN_SQL);

		/* uncomment after you create your customers database */
		/*
		beginTransactionStatement = customerConn.prepareStatement(BEGIN_TRANSACTION_SQL);
		commitTransactionStatement = customerConn.prepareStatement(COMMIT_SQL);
		rollbackTransactionStatement = customerConn.prepareStatement(ROLLBACK_SQL);
		*/

		/* add here more prepare statements for all the other queries you need */
		actorMidStatement = conn.prepareStatement(ACTOR_MID_SQL);
        remainingRentalsStatement = customerConn.prepareStatement(REMAINING_RENTALS_SQL);
        customerNameStatement = customerConn.prepareStatement(CUSTOMER_NAME_SQL);
        isValidMovieIdStatement = conn.prepareStatement(IS_VALID_MOVIE_ID_SQL);
        isValidPlanIdStatement = customerConn.prepareStatement(IS_VALID_PLAN_ID_SQL);
        customerIdFromRentalStatement = customerConn.prepareStatement(CUSTOMER_ID_FROM_RENTAL_SQL);
    }


    /**********************************************************/
    /* Suggested helper functions; you can complete these, or write your own
       (but remember to delete the ones you are not using!) */

    public int getRemainingRentals(int cid) throws Exception {
		/* How many movies can she/he still rent?
		   You have to compute and return the difference between the customer's plan
		   and the count of outstanding rentals */
        int remaining_rentals = 0;

        remainingRentalsStatement.clearParameters();
        remainingRentalsStatement.setInt(1, cid);
        remainingRentalsStatement.setInt(2, cid);
        ResultSet remaining_rentals_set = remainingRentalsStatement.executeQuery();
        if (remaining_rentals_set.next())
            remaining_rentals = remaining_rentals_set.getInt(1);
        remaining_rentals_set.close();

        return (remaining_rentals);
    }

    public String getCustomerName(int cid) throws Exception {
		/* Find the first and last name of the current customer. */
        String customer_name = null;

        customerNameStatement.clearParameters();
        customerNameStatement.setInt(1, cid);
        ResultSet customer_name_set = customerNameStatement.executeQuery();
        if (customer_name_set.next())
            customer_name = customer_name_set.getString(1) + " " + customer_name_set.getString(2);
        customer_name_set.close();

        return customer_name;
    }

    public boolean isValidPlan(int planid) throws Exception {
		/* Is planid a valid plan ID?  You have to figure it out */
        int plan_id_count = 0;

        isValidPlanIdStatement.clearParameters();
        isValidPlanIdStatement.setInt(1, planid);
        ResultSet is_valid_plan_set = isValidPlanIdStatement.executeQuery();
        if (is_valid_plan_set.next())
            plan_id_count = is_valid_plan_set.getInt(1);
        is_valid_plan_set.close();

        return (plan_id_count==1);
    }

    public boolean isValidMovie(int mid) throws Exception {
		/* is mid a valid movie ID?  You have to figure it out */
        int movie_id_count = 0;

        isValidMovieIdStatement.clearParameters();
        isValidMovieIdStatement.setInt(1, mid);
        ResultSet is_valid_movie_set = isValidMovieIdStatement.executeQuery();
        if (is_valid_movie_set.next())
            movie_id_count = is_valid_movie_set.getInt(1);
        is_valid_movie_set.close();

        return (movie_id_count==1);
    }

    private int getRenterID(int mid) throws Exception {
		/* Find the customer id (cid) of whoever currently rents the movie mid; return -1 if none */
        int renter_id = -1;

        customerIdFromRentalStatement.clearParameters();
        customerIdFromRentalStatement.setInt(1, mid);
        ResultSet renter_id_set = customerIdFromRentalStatement.executeQuery();
        if (renter_id_set.next())
            renter_id = renter_id_set.getInt(1);
        renter_id_set.close();

        return renter_id;
    }

    /**********************************************************/
    /* login transaction: invoked only once, when the app is started  */
    public int transaction_login(String name, String password) throws Exception {
		/* authenticates the user, and returns the user id, or -1 if authentication fails */

		int cid;

		customerLoginStatement.clearParameters();
		customerLoginStatement.setString(1,name);
		customerLoginStatement.setString(2,password);
		ResultSet cid_set = customerLoginStatement.executeQuery();
		if (cid_set.next()) cid = cid_set.getInt(1);
		else cid = -1;
		cid_set.close();

        if (TEST==1)
        {
            System.out.println("Hello " + getCustomerName(cid) + "!");
            System.out.println("You can rent " + getRemainingRentals(cid) + " more movies");
        }

        return(cid);
    }

    public void transaction_printPersonalData(int cid) throws Exception {
		/* println the customer's personal data: name, and plan number */

    }


    /**********************************************************/
    /* main functions in this project: */

    public void transaction_search(int cid, String movie_title)
            throws Exception {

		/* searches for movies with matching titles: SELECT * FROM movie WHERE name LIKE movie_title */
		/* prints the movies, directors, actors, and the availability status:
		   AVAILABLE, or UNAVAILABLE, or YOU CURRENTLY RENT IT */
        int count = 0;
        int acount = 0;
        ResultSet movie_set = null;

        searchStatement.clearParameters();
        searchStatement.setString(1, "%" + movie_title + "%");
        movie_set = searchStatement.executeQuery();
        count = 0;
        while (movie_set.next()) {
            int mid = movie_set.getInt(1);
            System.out.println(
                    " " + ++count +
                    " ID: " + mid + " NAME: "
                    + movie_set.getString(2) + " YEAR: "
                    + movie_set.getString(3));

			/* do a dependent join with directors */
            directorMidStatement.clearParameters();
            directorMidStatement.setInt(1, mid);
            ResultSet director_set = directorMidStatement.executeQuery();
            while (director_set.next()) {
                System.out.println("\t\tDirector: " + director_set.getString(3)
                        + " " + director_set.getString(2));
            }
            director_set.close();

			/* now you need to retrieve the actors, in the same manner */
            actorMidStatement.clearParameters();
            actorMidStatement.setInt(1, mid);
            ResultSet actor_set = actorMidStatement.executeQuery();
            while (actor_set.next()) {
                System.out.println("\t\t " + ++acount + " Actor: " + actor_set.getString(1)
                        + " " + actor_set.getString(2));
            }
            actor_set.close();


			/* then you have to find the status: of "AVAILABLE" "YOU HAVE IT", "UNAVAILABLE" */
            int temp_cid = getRenterID(mid);
            if (temp_cid == -1) {
                System.out.println("\t\tStatus: AVAILABLE");
            }
            else if (temp_cid == cid) {
                System.out.println("\t\tStatus: YOU HAVE IT");
            }
            else {
                System.out.println("\t\tStatus: UNAVAILABLE");
            }
        }
        movie_set.close();
        System.out.println();
    }

    public void transaction_choosePlan(int cid, int pid) throws Exception {
	    /* updates the customer's plan to pid: UPDATE customer SET plid = pid */
	    /* remember to enforce consistency ! */
    }

    public void transaction_listPlans() throws Exception {
	    /* println all available plans: SELECT * FROM plan */
    }

    public void transaction_rent(int cid, int mid) throws Exception {
	    /* rent the movie mid to the customer cid */
	    /* remember to enforce consistency ! */
    }

    public void transaction_return(int cid, int mid) throws Exception {
	    /* return the movie mid by the customer cid */
    }

    public void transaction_fastSearch(int cid, String movie_title)
            throws Exception {
		/* like transaction_search, but uses joins instead of dependent joins
		   Needs to run three SQL queries: (a) movies, (b) movies join directors, (c) movies join actors
		   Answers are sorted by mid.
		   Then merge-joins the three answer sets */
        int count = 0;

        fastSearchStatement.clearParameters();
        fastSearchStatement.setString(1, "%" + movie_title + "%");
        ResultSet movie_set = fastSearchStatement.executeQuery();
        if (false && TEST == 1){
        count = 0;
        while (movie_set.next()) {
            int mid = movie_set.getInt(1);
            System.out.println(
                    " " + ++count +
                    " ID: " + mid + " NAME: "
                    + movie_set.getString(2) + " YEAR: "
                    + movie_set.getString(3));
        }

        fastSearchDirectorsStatement.clearParameters();
        fastSearchDirectorsStatement.setString(1, "%" + movie_title + "%");
        ResultSet director_set = fastSearchDirectorsStatement.executeQuery();
        count = 0;
        int prev_mid = -1;
        while (director_set.next()) {
            int mid = director_set.getInt(1);
            if (mid != prev_mid) {
                System.out.println(
                        " " + ++count +
                        " ID: " + mid +
                        " NAME: " + director_set.getString(2) +
                        " YEAR: " + director_set.getString(3));
                if (director_set.getString(5) != null || director_set.getString(6) != null) {
                    System.out.println("\tDIRECTOR: " + director_set.getString(6) + " " + director_set.getString(5));
                }
            }
            else {
                if (director_set.getString(6) != null || director_set.getString(5) != null) {
                    System.out.println("\tDIRECTOR: " + director_set.getString(6) + " " + director_set.getString(5));
                }
            }
            prev_mid = mid;
        }
        }

        fastSearchActorsStatement.clearParameters();
        fastSearchActorsStatement.setString(1, "%" + movie_title + "%");
        ResultSet actor_set = fastSearchActorsStatement.executeQuery();
        count = 0;
        while (actor_set.next()) {
            int mid = actor_set.getInt(1);
            if (actor_set.getString(2) != null) {
                System.out.println(
                    " " + ++count +
                    " ID: " + mid +
                    " ACTOR: (" + actor_set.getString(2) + ") " +
                            actor_set.getString(3) + " " + actor_set.getString(4));
            }
        }


        actor_set.close();
//        director_set.close(); // TODO: open me
        movie_set.close();
        System.out.println();
    }


    /* Uncomment helpers below once you've got beginTransactionStatement,
       commitTransactionStatement, and rollbackTransactionStatement setup from
       prepareStatements():
    
       public void beginTransaction() throws Exception {
	    customerConn.setAutoCommit(false);
	    beginTransactionStatement.executeUpdate();	
        }

        public void commitTransaction() throws Exception {
	    commitTransactionStatement.executeUpdate();	
	    customerConn.setAutoCommit(true);
	}
        public void rollbackTransaction() throws Exception {
	    rollbackTransactionStatement.executeUpdate();
	    customerConn.setAutoCommit(true);
	    } 
    */

}

