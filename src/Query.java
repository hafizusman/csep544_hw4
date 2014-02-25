import java.sql.*;
import java.util.*;

import java.io.FileInputStream;

/**
 * Runs queries against a back-end database
 */
public class Query {
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

    private static final String SEARCH_MOVIE_SQL =
            "SELECT * FROM movie WHERE name LIKE ? ORDER BY id";
    private PreparedStatement searchStatement;

    private static final String SEARCH_DIRECTOR_SQL =
            "SELECT y.* " +
            "FROM movie_directors x, directors y " +
            "WHERE x.mid = ? and x.did = y.id";
    private PreparedStatement directorMidStatement;

    private static final String SEARCH_ACTOR_SQL =
            "SELECT A.fname, A.lname " +
            "FROM CASTS AS C " +
            "INNER JOIN MOVIE M ON M.id = C.mid " +
            "INNER JOIN ACTOR A ON A.id = C.pid " +
            "WHERE M.id = ? " +
            "GROUP BY A.id, A.fname, A.lname";
    private PreparedStatement actorMidStatement;

    private static final String CUSTOMER_LOGIN_SQL =
            "SELECT * FROM CUSTOMERS WHERE login = ? and password = ?";
    private PreparedStatement customerLoginStatement;

    private static final String REMAINING_RENTALS_SQL =
            "SELECT " +
            "(SELECT P.maxrentals FROM CUSTOMERS AS C INNER JOIN PLANS AS P ON P.id=C.plan_id WHERE C.id=? GROUP BY P.maxrentals) " +
            "- " +
            "(SELECT COUNT(*) FROM CUSTOMERS AS C INNER JOIN PLANS AS P ON P.id=C.plan_id INNER JOIN RENTALS AS R ON R.customerid=C.id WHERE C.id=? AND R.status=" + RENTAL_STATUS_OPENED + ")";
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
            "SELECT X.id AS mid, D.fname, D.lname " +
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

	private static final String BEGIN_TRANSACTION_SQL =
		    "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION;";
	private PreparedStatement beginTransactionStatement;

	private static final String COMMIT_SQL =
            "COMMIT TRANSACTION";
	private PreparedStatement commitTransactionStatement;

	private static final String ROLLBACK_SQL =
            "ROLLBACK TRANSACTION";
	private PreparedStatement rollbackTransactionStatement;

    private static final String SHOW_PLANS_SQL =
            "SELECT * FROM PLANS";
    private PreparedStatement plansStatement;

    private static final String GET_PLAN_SQL =
            "SELECT * FROM PLANS WHERE id = ?";
    private PreparedStatement getPlansStatement;

    private static final String GET_PLAN_INFO_FROM_CUSTOMERID_SQL =
            "SELECT C.plan_id, P.maxrentals " +
            "FROM PLANS AS P " +
            "INNER JOIN CUSTOMERS AS C ON C.plan_id=P.id " +
            "WHERE C.id=?";
    private PreparedStatement getPlansInfoForCustomerStatement;

    private static final String UPDATE_PLAN_SQL =
            "UPDATE CUSTOMERS SET plan_id=? WHERE id=?";
    private PreparedStatement updatePlanStatement;

    private static final String UPDATE_RENTAL_SQL =
            "INSERT INTO RENTALS VALUES (?, ?, " + RENTAL_STATUS_OPENED + ", SYSDATETIME())";
    private PreparedStatement updateRentalStatement;

    private static final String GET_RENTAL_STATUS_COUNT_SQL =
            "SELECT SUM(R.status) " +
            "FROM RENTALS AS R " +
            "WHERE R.movieid = ?";
    private PreparedStatement getRentalStatusCountStatement;

    private static final String RETURN_RENTAL_SQL =
            "UPDATE RENTALS " +
            "SET status=" + RENTAL_STATUS_CLOSED + " " +
            "WHERE customerid=? AND movieid=?";
    private PreparedStatement returnRentalStatement;


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
		//conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE); // TODO: needed?

		/* open connections to the customer DB database */
        jSQLUrl	   = configProps.getProperty("videostore.customer_url");
        customerConn = DriverManager.getConnection(jSQLUrl, // database
                        jSQLUser, // user
                        jSQLPassword); // password
        customerConn.setAutoCommit(true); //by default automatically commit after each statement
        //customerConn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE); // TODO: needed?
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

        searchStatement = conn.prepareStatement(SEARCH_MOVIE_SQL);
        directorMidStatement = conn.prepareStatement(SEARCH_DIRECTOR_SQL);
        actorMidStatement = conn.prepareStatement(SEARCH_ACTOR_SQL);
        fastSearchStatement = conn.prepareStatement(FAST_SEARCH_SQL);
        fastSearchDirectorsStatement = conn.prepareStatement(FAST_SEARCH_DIRECTORS_SQL);
        fastSearchActorsStatement = conn.prepareStatement(FAST_SEARCH_ACTORS_SQL);
        isValidMovieIdStatement = conn.prepareStatement(IS_VALID_MOVIE_ID_SQL);

        beginTransactionStatement = customerConn.prepareStatement(BEGIN_TRANSACTION_SQL);
        commitTransactionStatement = customerConn.prepareStatement(COMMIT_SQL);
        rollbackTransactionStatement = customerConn.prepareStatement(ROLLBACK_SQL);
        customerLoginStatement = customerConn.prepareStatement(CUSTOMER_LOGIN_SQL);
        remainingRentalsStatement = customerConn.prepareStatement(REMAINING_RENTALS_SQL);
        customerNameStatement = customerConn.prepareStatement(CUSTOMER_NAME_SQL);
        isValidPlanIdStatement = customerConn.prepareStatement(IS_VALID_PLAN_ID_SQL);
        customerIdFromRentalStatement = customerConn.prepareStatement(CUSTOMER_ID_FROM_RENTAL_SQL);
        plansStatement = customerConn.prepareStatement(SHOW_PLANS_SQL);
        getPlansStatement = customerConn.prepareStatement(GET_PLAN_SQL);
        getPlansInfoForCustomerStatement = customerConn.prepareStatement(GET_PLAN_INFO_FROM_CUSTOMERID_SQL);
        updatePlanStatement = customerConn.prepareStatement(UPDATE_PLAN_SQL);
        updateRentalStatement = customerConn.prepareStatement(UPDATE_RENTAL_SQL);
        getRentalStatusCountStatement = customerConn.prepareStatement(GET_RENTAL_STATUS_COUNT_SQL);
        returnRentalStatement = customerConn.prepareStatement(RETURN_RENTAL_SQL);
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

        return(cid);
    }

    public void transaction_printPersonalData(int cid) throws Exception {
		/* println the customer's personal data: name, and plan number */
        beginTransaction();

        String name = getCustomerName(cid);
        int remaining_rentals = getRemainingRentals(cid);

        commitTransaction();

        System.out.println("HELLO " + name + "!");
        System.out.println("REMAINING RENTALS: " + remaining_rentals);
    }


    /**********************************************************/
    /* main functions in this project: */

    public void transaction_search(int cid, String movie_title)
            throws Exception {

		/* searches for movies with matching titles: SELECT * FROM movie WHERE name LIKE movie_title */
		/* prints the movies, directors, actors, and the availability status:
		   AVAILABLE, or UNAVAILABLE, or YOU CURRENTLY RENT IT */
        ResultSet movie_set = null;

        searchStatement.clearParameters();
        searchStatement.setString(1, "%" + movie_title + "%");
        movie_set = searchStatement.executeQuery();
        while (movie_set.next()) {
            int mid = movie_set.getInt(1);
            System.out.println(
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
                System.out.println("\t\t" + "Actor: " + actor_set.getString(2)
                        + " " + actor_set.getString(1));
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
        int new_plan_max_rentals = 0;
        int current_plan_remaining_rentals = 0;
        int current_plan_max_rentals = 0;
        int current_plan_id = -1;
        int current_movies_rented = 0;

        beginTransaction();

        getPlansStatement.clearParameters();
        getPlansStatement.setInt(1, pid);
        ResultSet plan_set = getPlansStatement.executeQuery();
        plan_set.next();
        new_plan_max_rentals = plan_set.getInt(3);
        plan_set.close();

        getPlansInfoForCustomerStatement.clearParameters();
        getPlansInfoForCustomerStatement.setInt(1, cid);
        ResultSet plan_info_set = getPlansInfoForCustomerStatement.executeQuery();
        plan_info_set.next();
        current_plan_id = plan_info_set.getInt(1);
        current_plan_max_rentals = plan_info_set.getInt(2);
        plan_info_set.close();

        current_plan_remaining_rentals = getRemainingRentals(cid);

        updatePlanStatement.clearParameters();
        updatePlanStatement.setInt(1, pid);
        updatePlanStatement.setInt(2, cid);
        updatePlanStatement.executeUpdate();

        if (current_plan_id == pid)
        {
            System.out.println("SAME PLAN: noop!");

            rollbackTransaction();
        }
        else
        {
            current_movies_rented = current_plan_max_rentals - current_plan_remaining_rentals;
            System.out.println(
                    "newID=" + pid +
                            " newMax=" + new_plan_max_rentals +
                            " oldID=" + current_plan_id +
                            " oldMax=" + current_plan_max_rentals +
                            " oldDelta=" + current_plan_remaining_rentals +
                            " currentRented=" + current_movies_rented);

            if (current_movies_rented <= new_plan_max_rentals) {
                commitTransaction();
                System.out.println("PLAN UPDATED TO !!" + pid);
            }
            else {
                rollbackTransaction();
                System.out.println("PLAN NOT UPDATED TO !!" + pid);
                System.out.println("Please return some movies");
            }
        }

        exit:
            return;
    }

    public void transaction_listPlans() throws Exception {
	    /* println all available plans: SELECT * FROM plan */
        plansStatement.clearParameters();
        ResultSet plan_set = plansStatement.executeQuery();

        while(plan_set.next()) {
            System.out.println(
                    "ID: " + plan_set.getInt(1) + " " +
                            "NAME: " + plan_set.getString(2) + " " +
                            "RENTALS: " + plan_set.getInt(3) + " " +
                            "FEE: " + plan_set.getInt(4));
        }
        plan_set.close();
    }

    public void transaction_rent(int cid, int mid) throws Exception {
	    /* rent the movie mid to the customer cid */
	    /* remember to enforce consistency ! */

        beginTransaction();

        boolean is_valid_movie = isValidMovie(mid);
        int rentals_remaining = getRemainingRentals(cid);

        updateRentalStatement.clearParameters();
        updateRentalStatement.setInt(1, cid);
        updateRentalStatement.setInt(2, mid);
        updateRentalStatement.executeUpdate();

        getRentalStatusCountStatement.clearParameters();
        getRentalStatusCountStatement.setInt(1, mid);
        ResultSet rental_status_count_set = getRentalStatusCountStatement.executeQuery();
        rental_status_count_set.next();

        int rental_status_count = rental_status_count_set.getInt(1);
        rental_status_count_set.close();

        if (is_valid_movie == false
            || rental_status_count != 1
            || rentals_remaining == 0) {
            rollbackTransaction();
            System.out.println("ROLLED BACK RENTAL TRANS..." + is_valid_movie + " " + rental_status_count + " " + rentals_remaining);
        }
        else {
            commitTransaction();
            System.out.println("COMMITED RENTAL TRANS...");
        }
    }

    public void transaction_return(int cid, int mid) throws Exception {
	    /* return the movie mid by the customer cid */
        beginTransaction();

        boolean is_valid_movie = isValidMovie(mid);
        int remaining_rentals_before = getRemainingRentals(cid);

        returnRentalStatement.clearParameters();
        returnRentalStatement.setInt(1, cid);
        returnRentalStatement.setInt(2, mid);
        returnRentalStatement.executeUpdate();

        int remaining_rentals_after = getRemainingRentals(cid);

        if (is_valid_movie == false
            || (remaining_rentals_after - 1) != remaining_rentals_before) {
            rollbackTransaction();
            System.out.println("ROLLED BACK RETURN TRANS..." + is_valid_movie + " " + remaining_rentals_before + " " + remaining_rentals_after);
        }
        else {
            commitTransaction();
            System.out.println("COMMITED RETURN TRANS...");
        }
    }

    public void transaction_fastSearch(int cid, String movie_title)
            throws Exception {
		/* like transaction_search, but uses joins instead of dependent joins
		   Needs to run three SQL queries: (a) movies, (b) movies join directors, (c) movies join actors
		   Answers are sorted by mid.
		   Then merge-joins the three answer sets */
        int mid = -1, prev_mid = -1;

        fastSearchStatement.clearParameters();
        fastSearchStatement.setString(1, "%" + movie_title + "%");
        ResultSet movie_set = fastSearchStatement.executeQuery();
        HashMap<Integer, String> movies = new HashMap<Integer, String>();
        while(movie_set.next()) {
            movies.put( movie_set.getInt(1),
                        ("NAME: " + movie_set.getString(2) + " YEAR: " + movie_set.getString(3)));
        }
        movie_set.close();

        fastSearchDirectorsStatement.clearParameters();
        fastSearchDirectorsStatement.setString(1, "%" + movie_title + "%");
        ResultSet director_set = fastSearchDirectorsStatement.executeQuery();
        HashMap<Integer, ArrayList<String>> movie_directors = new HashMap<Integer, ArrayList<String>>();
        ArrayList<String> directors = null;
        mid = -1;
        prev_mid = -1;
        while(director_set.next()) {
            mid = director_set.getInt(1);
            if (mid != prev_mid) {
                if (prev_mid != -1) {
                    movie_directors.put(prev_mid, directors);
                }
                directors = new ArrayList<String>();
                if (director_set.getString(3) != null || director_set.getString(2) != null) {
                    directors.add( director_set.getString(3) + " " +  director_set.getString(2));
                }
            }
            else {
                if (director_set.getString(3) != null || director_set.getString(2) != null) {
                    directors.add( director_set.getString(3) + " " +  director_set.getString(2));
                }
            }
            prev_mid = mid;
        }
        if (prev_mid != -1) {
            movie_directors.put(prev_mid, directors);
        }
        director_set.close();

        fastSearchActorsStatement.clearParameters();
        fastSearchActorsStatement.setString(1, "%" + movie_title + "%");
        ResultSet actor_set = fastSearchActorsStatement.executeQuery();
        HashMap<Integer, ArrayList<String>> movie_actors = new HashMap<Integer, ArrayList<String>>();
        ArrayList<String> actors = null;
        mid = -1;
        prev_mid = -1;
        while(actor_set.next()) {
            mid = actor_set.getInt(1);
            if (mid != prev_mid) {
                if (prev_mid != -1) {
                    movie_actors.put(prev_mid, actors);
                }
                actors = new ArrayList<String>();
                if (actor_set.getString(4) != null || actor_set.getString(3) != null) {
                    actors.add( actor_set.getString(4) + " " +  actor_set.getString(3));
                }
            }
            else {
                if (actor_set.getString(4) != null || actor_set.getString(3) != null) {
                    actors.add( actor_set.getString(4) + " " +  actor_set.getString(3));
                }
            }
            prev_mid = mid;
        }
        if (prev_mid != -1) {
            movie_actors.put(prev_mid, actors);
        }
        actor_set.close();

        for(Map.Entry<Integer, String> m_entry : movies.entrySet()) {
            Integer m_id = m_entry.getKey();
            String value = m_entry.getValue();
            System.out.println("ID: " + m_id + " " + value);

            ArrayList <String> temp = movie_directors.get(m_id);
            for (String s : temp) {
                System.out.println("\t\tDirector: " + s);
            }

            temp = movie_actors.get(m_id);
            for (String s : temp) {
                System.out.println("\t\tActor: " + s);
            }
        }
        System.out.println();
    }

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
}

