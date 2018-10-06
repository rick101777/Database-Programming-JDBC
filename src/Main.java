import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/*
 * 	Ricardo Farias
 * 	Database Programming Final Project
 */


public class Main {
	
	
	
	public static void main(String[] args) {
		DatabaseCommunicator communicate = new DatabaseCommunicator();
		communicate.establishConnection();
		communicate.dropTables();
		communicate.createTables();
		
		communicate.insertAge();
		communicate.insertOccupation();
		
		FileParser parser = new FileParser("users.dat");
		
		String[] userResult = parser.parse();
		communicate.insertUsers(userResult);
		
		
		parser.setFileName("movies.dat");
		String[] moviesResult = parser.parse();
		communicate.insertMovies(moviesResult);
		
		parser.setFileName("ratings.dat");
		String[] ratingsResult = parser.parse();
		communicate.insertRatings(ratingsResult);
		
		communicate.interestingQuery();
		communicate.terminateConnection();
		
	}
}
