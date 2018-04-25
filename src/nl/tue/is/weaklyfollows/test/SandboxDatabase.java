package nl.tue.is.weaklyfollows.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

import nl.tue.util.StringPadding;

public class SandboxDatabase {

	private static final String CONNECTION_STRING = "jdbc:h2:./temp/tempdb";
	Connection conn;
	Statement stat;
	long startTime;

	public SandboxDatabase() throws ClassNotFoundException, SQLException{
		Class.forName("org.h2.Driver");
		conn = DriverManager.getConnection(CONNECTION_STRING, "sa", "");
		stat = conn.createStatement();
	}
	
	public void close() throws SQLException{
		stat.close();
		conn.close();
	}
		
	public void printResultSet(String query) throws SQLException{
		ResultSet rs = stat.executeQuery(query);
		for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++){
			//System.out.print(StringPadding.rpad(rs.getMetaData().getColumnName(i),40));
			System.out.print(StringPadding.rpad(rs.getMetaData().getColumnDisplaySize(i),40));
		}
		System.out.println();
		while (rs.next()){
			StringBuilder sb = new StringBuilder();
			ResultSetMetaData rsmd = rs.getMetaData();
			int numberOfColumns = rsmd.getColumnCount();
			for (int i = 1; i <= numberOfColumns; i++) {
				sb.append(StringPadding.rpad(rs.getString(i),40));
			}
			String data = sb.toString();
			System.out.println(data);
		}
	}
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		SandboxDatabase sd = new SandboxDatabase();
		Scanner sc = new Scanner(System.in);
		String read = sc.nextLine();
        while(!read.equals("quit")) {
        	try {
        		sd.printResultSet(read);
        	} catch (SQLException e) {
        		System.out.println(e.getMessage());
        	}
        	read = sc.nextLine();
        }
        sc.close();
        sd.close();
	}

}
