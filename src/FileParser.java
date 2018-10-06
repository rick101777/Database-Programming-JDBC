import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class FileParser {
	
	private static final String DIRECTORY_PATH = "src/DataFiles/";
	private String filename = "movies.dat";
	
	public FileParser() {
		
	}

	public FileParser(String fileName){
		this.filename = fileName;
	}
	
/*-----------------------------------------------Getters and Setters-------------------------------------------------------------------*/
	public void setFileName(String newFileName) {
		this.filename = newFileName;
	}
	
	public String getFileName() {
		return this.filename;
	}

/*------------------------------------------------------------------------------------------------------------------------------------------------*/
	
	// Calculates the length of the dat file
	private int calculateLength() {
		int count = 0;
		String line;
		BufferedReader buff = null;
		try {
			File file = new File(DIRECTORY_PATH + filename);
			FileReader reader = new FileReader(file);
			buff = new BufferedReader(reader);
			while ((line = buff.readLine()) != null) {
				count++;
			}
		}catch(IOException io) {
			System.err.println("Calculate File Length Error: " + io.getMessage());
		}finally {
			try {
				if (buff != null) {
					buff.close();
				}
			}catch(IOException io) {
				System.err.println("calculateLength Error: " + io.getMessage());
			}
		}
		return count;
	}
	
	// Extracts data from dat files and puts the data into a string array
	public  String[]  parse() {
		String[] result = null;
		try {
			File file = new File(DIRECTORY_PATH + filename);
			FileReader reader = new FileReader(file);
			BufferedReader buff = new BufferedReader(reader);
			int length = this.calculateLength();
			result = new String[length];
			String line;
			int index = 0;
			while ((line = buff.readLine()) != null) {
				result[index] = line;
				index++;
			}
			buff.close();
		}catch(IOException io) {
			System.err.println("Parse Movies Error: " + io.getMessage());
		}
		
		return result;
	}
	
}
