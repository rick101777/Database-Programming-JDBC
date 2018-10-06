import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.StringTokenizer;
import java.util.TimeZone;

public class DatabaseCommunicator {
	
	private Connection conn = null;
	private String username = "-------";
	private String password = "-------";
	private String ipAddress = "---.---.---";
	private String port = "---------";

	// Default construtor
	public DatabaseCommunicator() {}
	
	
	// Username and Password can be given, to login as a different user
	public DatabaseCommunicator(String username, String password) {
		this.username = username;
		this.password = password;
	}
	
	// Additional connectivity options
	public DatabaseCommunicator(String username, String password, String ipAddress, String port) {
		this.username = username;
		this.password = password;
		this.ipAddress = ipAddress;
		this.port = port;
	}
	
/*--------------------------------------------------------Getters and Setters--------------------------------------------------------------*/	
	
	public void setUsername(String username) {
		this.username = username;
	}
	
	public void setPassword(String password) {
		this.password = password;
	}
	
	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}
	
	public void setPort(String port) {
		this.port = port;
	}

	public String getUsername() {
		return this.username;
	}
	
	public String getPassword() {
		return this.password;
	}
	
	public String getIpAddress() {
		return this.ipAddress;
	}
	
	public String getPort() {
		return this.port;
	}
	
/*--------------------------------------------------------------------------------------------------------------------------------------------------------------*/

	// Declares the jdbc class for oracle
	static {
		try {
			Class.forName("oracle.jdbc.OracleDriver");
		}catch(ClassNotFoundException e) {
			System.out.println(e);
			System.exit(1);
		}
	}
	
	// Establishes a connection to the database and retruns true if successful
	public boolean establishConnection() {
		try {
			this.conn = DriverManager.getConnection("jdbc:oracle:thin:@" + ipAddress + ":" + port + ":def", username, password);
		}catch (SQLException sql) {
			System.err.println("Connection was unable to be established");
			System.err.println("Username or password incorrect, or internet connection not found");
		}
		
		if (conn.equals(null)) {
			return false;
		}
		System.out.println("Connection to database has been succesfully established");
		return true;
	}
	
	// Closes the connection to the database and returns true if successful
	public boolean terminateConnection() {
		try {
			if (conn != null) {
				this.conn.close();
			}
			if (this.conn.isClosed()) {
				return false;
			}
		}catch(SQLException sql) {
			System.err.println("Connection was unable to be terminated: " + sql.getMessage());
		}
		return true;
	}
	
	// Creates tables in the database
	public void createTables() {
		Statement createTable = null;
		try {
			String createMovies = ""
					+ "CREATE TABLE movies (movieid NUMBER PRIMARY KEY, "
							+ "movietitle VARCHAR2(90),"
							+ " year INT)";
			String createRatings = ""
					+ "CREATE TABLE ratings (userid INT, "
							+ "movieid INT, "
							+ "rating INT, "
							+ "timestamp TIMESTAMP, "
							+ "PRIMARY KEY(userid, movieid),"
							+ "FOREIGN KEY (userid) REFERENCES users(userid),"
							+ "FOREIGN KEY (movieid) REFERENCES movies(movieid))";
			String createUsers = ""
					+ "CREATE TABLE users (userid NUMBER(10) PRIMARY KEY, "
							+ "gender CHAR(1), "
							+ "agecode INT, "
							+ "occupation INT, "
							+ "zipcode VARCHAR2(12),"
							+ "FOREIGN KEY (agecode) REFERENCES age(agecode),"
							+ "FOREIGN KEY (occupation) REFERENCES occupation(occupation))";
			String createMovieGenre = ""
					+ "CREATE TABLE MOVIE_GENRE (MOVIEID INT,"
							+"GENRE VARCHAR2(20),"
							+"PRIMARY KEY (MOVIEID, GENRE)," 
							+"FOREIGN KEY (MOVIEID) REFERENCES MOVIES(MOVIEID))";
			String createAgeTable = ""
					+ "CREATE TABLE age (AGECODE INT, "
							+ "RANGE VARCHAR2(10), "
							+ "PRIMARY KEY (AGECODE))";
			String createOccupationTable = ""
					+ "CREATE TABLE occupation (occupation INT PRIMARY KEY, "
							+ "description VARCHAR2(40))";
			
			createTable = conn.createStatement();
			createTable.execute(createMovies);
			createTable.execute(createMovieGenre);
			createTable.execute(createAgeTable);
			createTable.execute(createOccupationTable);
			createTable.execute(createUsers);
			createTable.execute(createRatings);
			
			System.out.println("Successfully created all tables");
			
		}catch(SQLException sql) {
			System.err.println("Create Table Error: " + sql.getMessage());
		}finally {
			try {
				createTable.close();
			}catch(SQLException sql) {
				System.out.println(sql.getMessage());
			}
		}
	}
	
	// Drops all tables from the database
	public void dropTables() {
		Statement dropTables = null;
		try {
			String dropMovies = "DROP TABLE movies ";
			String dropRatings = "DROP TABLE ratings";
			String dropUsers = "DROP TABLE users ";
			String dropMovieGenre = "DROP TABLE movie_genre";
			String dropAgeTable = "DROP TABLE age";
			String dropOccupationTable = "DROP TABLE occupation";
			dropTables = conn.createStatement();
			
			dropTables.executeUpdate(dropMovieGenre);
			dropTables.executeUpdate(dropRatings);
			dropTables.executeUpdate(dropUsers);
			dropTables.executeUpdate(dropAgeTable);
			dropTables.executeUpdate(dropOccupationTable);
			dropTables.executeUpdate(dropMovies);
			
			System.out.println("Successfully dropped all tables");
			
		}catch (SQLException sql) {
			System.err.println("Drop Table Error: " + sql.getMessage());
		}finally {
			try {
				if (dropTables != null) {
					dropTables.close();
				}
			}catch(SQLException sql) {
				
			}
		}
	}
	
	// Inserts agecodes and their appropriate descriptions into age table
	public void insertAge() {
		PreparedStatement insertAge = null;
		try {
			insertAge = conn.prepareStatement("INSERT INTO age(agecode, range) VALUES(?, ?)");
			insertAge.setInt(1, 1);
			insertAge.setString(2, "Under 18");
			insertAge.executeUpdate();
			
			insertAge.setInt(1, 18);
			insertAge.setString(2, "18-24");
			insertAge.executeUpdate();
			
			insertAge.setInt(1, 25);
			insertAge.setString(2, "25-34");
			insertAge.executeUpdate();
			
			insertAge.setInt(1, 35);
			insertAge.setString(2, "35-44");
			insertAge.executeUpdate();
			
			insertAge.setInt(1, 45);
			insertAge.setString(2, "45-49");
			insertAge.executeUpdate();
			
			insertAge.setInt(1, 50);
			insertAge.setString(2, "50-55");
			insertAge.executeUpdate();
			
			insertAge.setInt(1, 56);
			insertAge.setString(2, "56+");
			insertAge.executeUpdate();
			
			System.out.println("insertAge inserted:  7  rows into age successfully");
		} catch(SQLException sql) {
			System.err.println("insertAge has encountered a SQL error: " + sql.getMessage());
		}finally {
			try {
				if (insertAge != null) {
					insertAge.close();
				}
			} catch(SQLException sql) {
				System.err.println("insertAge has encountered a statement closing error: " + sql.getMessage());
			}
		}
	}
	
	// Inserts occupation and description into occupation table
	public void insertOccupation() {
		PreparedStatement insertOccupation = null;
		try {
			insertOccupation = conn.prepareStatement("INSERT INTO occupation(occupation, description) VALUES(?, ?)");
			int rowCount = 0;
			for (int i = 0; i <= 20; i++) {
				switch(i) {
				case 0:
					insertOccupation.setInt(1, i);
					insertOccupation.setString(2, "other");
					insertOccupation.executeUpdate();
					rowCount++;
					break;
				case 1:
					insertOccupation.setInt(1, i);
					insertOccupation.setString(2, "academic/educator");
					insertOccupation.executeUpdate();
					rowCount++;
					break;
				case 2:
					insertOccupation.setInt(1, i);
					insertOccupation.setString(2, "artist");
					insertOccupation.executeUpdate();
					rowCount++;
					break;
				case 3:
					insertOccupation.setInt(1, i);
					insertOccupation.setString(2, "clerical/admin");
					insertOccupation.executeUpdate();
					rowCount++;
					break;
				case 4:
					insertOccupation.setInt(1, i);
					insertOccupation.setString(2, "college/grad student");
					insertOccupation.executeUpdate();
					rowCount++;
					break;
				case 5:
					insertOccupation.setInt(1, i);
					insertOccupation.setString(2, "customer service");
					insertOccupation.executeUpdate();
					rowCount++;
					break;
				case 6:
					insertOccupation.setInt(1, i);
					insertOccupation.setString(2, "doctor/health care");
					insertOccupation.executeUpdate();
					rowCount++;
					break;
				case 7:
					insertOccupation.setInt(1, i);
					insertOccupation.setString(2, "executive/managerial");
					insertOccupation.executeUpdate();
					rowCount++;
					break;
				case 8:
					insertOccupation.setInt(1, i);
					insertOccupation.setString(2, "farmer");
					insertOccupation.executeUpdate();
					rowCount++;
					break;
				case 9:
					insertOccupation.setInt(1, i);
					insertOccupation.setString(2, "homemaker");
					insertOccupation.executeUpdate();
					rowCount++;
					break;
				case 10:
					insertOccupation.setInt(1, i);
					insertOccupation.setString(2, "k-12 student");
					insertOccupation.executeUpdate();
					rowCount++;
					break;
				case 11:
					insertOccupation.setInt(1, i);
					insertOccupation.setString(2, "lawyer");
					insertOccupation.executeUpdate();
					rowCount++;
					break;
				case 12:
					insertOccupation.setInt(1, i);
					insertOccupation.setString(2, "programmer");
					insertOccupation.executeUpdate();
					rowCount++;
					break;
				case 13:
					insertOccupation.setInt(1, i);
					insertOccupation.setString(2, "retired");
					insertOccupation.executeUpdate();
					rowCount++;
					break;
				case 14:
					insertOccupation.setInt(1, i);
					insertOccupation.setString(2, "sales/marketing");
					insertOccupation.executeUpdate();
					rowCount++;
					break;
				case 15:
					insertOccupation.setInt(1, i);
					insertOccupation.setString(2, "scientist");
					insertOccupation.executeUpdate();
					rowCount++;
					break;
				case 16:
					insertOccupation.setInt(1, i);
					insertOccupation.setString(2, "self-employed");
					insertOccupation.executeUpdate();
					rowCount++;
					break;
				case 17:
					insertOccupation.setInt(1, i);
					insertOccupation.setString(2, "technician/engineer");
					insertOccupation.executeUpdate();
					rowCount++;
					break;
				case 18:
					insertOccupation.setInt(1, i);
					insertOccupation.setString(2, "tradesman/craftsman");
					insertOccupation.executeUpdate();
					rowCount++;
					break;
				case 19:
					insertOccupation.setInt(1, i);
					insertOccupation.setString(2, "unemployed");
					insertOccupation.executeUpdate();
					rowCount++;
					break;
				case 20:
					insertOccupation.setInt(1, i);
					insertOccupation.setString(2, "writer");
					insertOccupation.executeUpdate();
					rowCount++;
					break;
				}
			}
			System.out.println("insertOccupation inserted:  " + rowCount + " rows inserted successfully into occupation");
		} catch(SQLException sql) {
			System.err.println("insertOccupation has encountered a SQL error: " + sql.getMessage());
		}finally {
			try {
				if (insertOccupation != null) {
					insertOccupation.close();
				}
			}catch(SQLException sql) {
				System.err.println("insertOccupation encountered a statement closing error: " + sql.getMessage());
			}
		}
	}
	
	
	
	
	// Inserts values into movies, movie_genre tables
	public void insertMovies(String[] movieData) {
		PreparedStatement insertMovie = null;
		PreparedStatement insertGenre = null;
		try {
			insertMovie = conn.prepareStatement("INSERT INTO movies(movieid, movietitle, year) VALUES(?, ?, ?)");
			insertGenre = conn.prepareStatement("INSERT INTO movie_genre(movieid, genre) VALUES(?, ?)");
			
			int movieRowCount = 0;
			int genreRowCount = 0;
			int length = movieData.length-1;
			for (int i = 0; i < length; i++) {
				StringTokenizer token = new StringTokenizer(movieData[i], "::");
				int movieid = 0;
				String movieTitle = "";
				int year = 0;
				String genre = "";
				try {
					movieid = Integer.parseInt(token.nextToken());
					String temp = token.nextToken();
					if (temp.contains("(")) {
						String[] temp2 = temp.split("\\(");
						if (temp2.length == 2) {
							movieTitle = temp2[0];
							year = Integer.parseInt(temp2[1].replace(")", ""));
							
							insertMovie.setInt(1, movieid);
							insertMovie.setString(2, movieTitle);
							insertMovie.setInt(3, year);
							insertMovie.executeUpdate();
							movieRowCount++;
							
							String[] listGenre = token.nextToken().split("\\|");
							int tempGenrelength = listGenre.length-1;
							for (int j = 0; j < tempGenrelength; j++) {
								genre = listGenre[j];
								insertGenre.setInt(1, movieid);
								insertGenre.setString(2, genre);
								insertGenre.executeUpdate();
								genreRowCount++;
							}
						}
						if (temp2.length == 3) {
							movieTitle = temp2[0] + "(" + temp2[1];
							year = Integer.parseInt(temp2[2].replace(")", ""));
							
							insertMovie.setInt(1, movieid);
							insertMovie.setString(2, movieTitle);
							insertMovie.setInt(3, year);
							insertMovie.executeUpdate();
							movieRowCount++;
							
							String[] listGenre = token.nextToken().split("\\|");
							int tempGenrelength = listGenre.length;
							for (int j = 0; j < tempGenrelength; j++) {
								genre = listGenre[j];
								insertGenre.setInt(1, movieid);
								insertGenre.setString(2, genre);
								insertGenre.executeUpdate();
								genreRowCount++;
							}
						}
					}
				}catch(Exception e) {
					System.err.println("insertMovies encountered String -> Int Conversion error: " + e.getMessage());
				}
				if (movieRowCount%50 == 0) {
					System.out.println("insertMovies completion percentage................ " +Math.round( ((float)(movieRowCount*100)/length)) + "%" );
				}
			}
			System.out.println("insertUsers inserted:  " + movieRowCount + " rows into movies successfully");
			System.out.println("insertUsers inserted:  " + genreRowCount + " rows into movie_genre successfully");
			
		}catch (SQLException sql) {
			System.err.println("insertMovies has encountered a SQL error: " + sql.getMessage());
		}finally {
			try {
				if (insertMovie != null) {
					insertMovie.close();
				}
				if (insertGenre != null) {
					insertGenre.close();
				}
			}catch(SQLException sql) {
				System.err.println("insertMovies has encountered a statement closing error: " + sql.getMessage());
			}
		}
	}
	
	// Inserts values into users, age, occupation tables
	public void insertUsers(String[] userData) {
		PreparedStatement insertUser = null;
		try {
			insertUser = conn.prepareStatement("INSERT INTO users(userid, gender, agecode, occupation, zipcode) VALUES (?, ?, ?, ?, ?)");
			int length = userData.length-1;
			int rowCount = 0;
			for (int i = 0; i < length; i++) {
				StringTokenizer token = new StringTokenizer(userData[i], "::");
				int userid = 0;
				String gender = "";
				int agecode = 0;
				int occupation = 0;
				String zipcode = "";
				try {
					userid = Integer.parseInt(token.nextToken());
					gender = token.nextToken();
					agecode = Integer.parseInt(token.nextToken());
					occupation = Integer.parseInt(token.nextToken());
					zipcode = token.nextToken();
				}catch (Exception e) {
					System.err.println("insertMovies encountered String -> Int Conversion  error: " + e.getMessage());
				}
				insertUser.setInt(1, userid);
				insertUser.setString(2, gender);
				insertUser.setInt(3, agecode);
				insertUser.setInt(4, occupation);
				insertUser.setString(5, zipcode);
				
				insertUser.executeUpdate();
				rowCount++;
				if (rowCount%50 == 0) {
					System.out.println("insertUser completion percentage................ " +Math.round( ((float)(rowCount*100)/length)) + "%" );
				}
			}
			System.out.println("insertUsers inserted:  " + rowCount + " rows into users successfully");
		} catch(SQLException sql) {
			System.err.println("insertUsers encountered SQL error: " + sql.getMessage());
		}finally {
			try {
				if (insertUser != null) {
					insertUser.close();
				}
			}catch(SQLException sql) {
				System.err.println("insertUser has encountered a statement closing error: " + sql.getMessage());
			}
		}
	}
	
	// Inserts values into ratings table
	public void insertRatings(String[] ratingData) {
		PreparedStatement insertRating = null;
		try {
			insertRating = conn.prepareStatement("INSERT INTO ratings(userid, movieid, rating, timestamp) VALUES(?, ?, ?, TO_TIMESTAMP(?, 'dd/MM/yyyy HH24:mi:ss'))");
			int rowCount = 0;
			int length = ratingData.length-1;
			for (int i = 0; i < length; i++) {
				StringTokenizer token = new StringTokenizer(ratingData[i], "::");
				int userid = 0;
				int movieid = 0;
				int rating = 0;
				long seconds = 0;
				try {
					userid = Integer.parseInt(token.nextToken());
					movieid = Integer.parseInt(token.nextToken());
					rating = Integer.parseInt(token.nextToken());
					seconds = Integer.parseInt(token.nextToken());
					seconds = seconds * 1000;
					Date date = new Date((seconds));
					DateFormat format = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
					format.setTimeZone(TimeZone.getTimeZone("ETC/UTC"));
					String formatted = format.format(date);
					
					insertRating.setInt(1, userid);
					insertRating.setInt(2, movieid);
					insertRating.setInt(3, rating);
					insertRating.setString(4, formatted);
					insertRating.executeUpdate();
					rowCount++;
					
					if (rowCount%50 == 0) {
						System.out.println("insertRatings completion percentage................ " +Math.round( ((float)(rowCount*100)/length)) + "%" );
					}
					
				} catch(Exception e) {
					//System.err.println("insertRatings encountered a String -> Int Conversion error: " + e.getMessage());
				}
			}
			System.out.println("insertRatings inserted:  " + rowCount + " rows into ratings successfully");
		}catch(SQLException sql) {
			System.err.println("insertRatings has encounted a SQL error: " + sql.getMessage());
		}finally {
			try {
				if (insertRating != null) {
					insertRating.close();
				}
			}catch(SQLException sql) {
				System.err.println("insertRatings has encounted a statement closing error: " + sql.getMessage());
			}
		}
	}
	
	
	// Conducts a query that results in an interesting fact about the data
	public void interestingQuery() {
		PreparedStatement interestingQuery = null;
		ResultSet Result = null;
		try {
			interestingQuery = conn.prepareStatement("SELECT" + 
					"     AVG(ratings.rating) AS average" + 
					" FROM" + 
					"     movies" + 
					"     INNER JOIN ratings ON ratings.movieid = movies.movieid" + 
					"     INNER JOIN users ON users.userid = ratings.userid" + 
					" WHERE" + 
					"     movies.movieid = (" + 
					"         SELECT" + 
					"             movie_genre.movieid" + 
					"         FROM" + 
					"             movie_genre" + 
					"         WHERE" + 
					"             movie_genre.genre ='Children''s'" + 
					"             AND movies.movieid = movie_genre.movieid" + 
					"     )" + 
					"     AND users.gender = ?");
			interestingQuery.setString(1, "F");
			Result = interestingQuery.executeQuery();
			Result.next();
			float femaleAverage = Result.getFloat(1);
			interestingQuery.setString(1, "M");
			Result = interestingQuery.executeQuery();
			Result.next();
			float maleAverage = Result.getFloat(1);
			System.out.println("Question: What gender on average gives children's movies a greater rating?");
			if (maleAverage > femaleAverage) {
				System.out.println("On average Males ( " + maleAverage + ") give children's movies a better rating than females (" + femaleAverage + ")");
			}
			if (maleAverage == femaleAverage) {
				System.out.println("On average Males ( " + maleAverage + ") and females (" + femaleAverage + ") give children's movies the same rating");
			}
			else {
				System.out.println("On average Females ( " + femaleAverage + ") give children's movies a better rating than Males (" + maleAverage + ")");
			}
		} catch(SQLException sql){
			System.err.println("interestingQuery has encountered a SQL error: " + sql.getMessage());
		}finally {
			try {
				if (interestingQuery != null) {
					interestingQuery.close();
				}
				if (Result != null) {
					Result.close();
				}
			}catch(SQLException sql) {
				System.err.println("interestingQuery has encountered a statement closing error: " + sql.getMessage());
			}
		}
	}
	
	
	
	
}
