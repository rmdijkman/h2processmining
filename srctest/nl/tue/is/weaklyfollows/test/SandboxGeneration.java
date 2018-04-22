package nl.tue.is.weaklyfollows.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import nl.tue.loggeneration.MarkovGeneration;

public class SandboxGeneration {

	Connection conn;
	Statement stat;
	long startTime;

	public SandboxGeneration() throws ClassNotFoundException, SQLException{
		Class.forName("org.h2.Driver");
		conn = DriverManager.getConnection("jdbc:h2:./tempdb", "sa", "");
		stat = conn.createStatement();
	}
	
	public void close() throws SQLException{
		stat.close();
		conn.close();
	}
	
	public void loadLog(String logName) throws SQLException{
		stat.execute("CREATE TABLE IF NOT EXISTS " + logName
				+ " AS SELECT "
				+ "CaseID,"
				+ "Activity,"
				+ "convert(parseDateTime(CompleteTimestamp,'yyyy/MM/dd hh:mm:ss'),TIMESTAMP) AS CompleteTimestamp,"
				+ "Variant,"
				+ "VariantIndex "
				+ "FROM CSVREAD('./resources/"+logName+".csv', null, 'fieldSeparator=;');");
		stat.execute("CREATE INDEX IF NOT EXISTS CaseID_idx ON "+logName+"(CaseID)");
		stat.execute("CREATE INDEX IF NOT EXISTS Activity_idx ON "+logName+"(Activity)");
		stat.execute("CREATE INDEX IF NOT EXISTS CompleteTimestamp_idx ON "+logName+"(CompleteTimestamp)");

		//Execute a simple query first, because otherwise the indexes do not seem to be initialized, 
		//which causes a very slow response in the nested query.
		stat.executeQuery(
				"  SELECT DISTINCT a.Activity, b.Activity "
				+ "FROM "+logName+" a, "+logName+" b "
				+ "WHERE a.CaseID = b.CaseID AND a.CompleteTimestamp < b.CompleteTimestamp"
				);
	}
	
	public void printTableSize(String tableName) throws SQLException{
		System.out.println("Statistics for " + tableName + ":");
		ResultSet rs = stat.executeQuery("SELECT COUNT(*) FROM " + tableName);
		rs.next();
		System.out.println("\tnumber of events:\t" + rs.getInt(1));
		
		rs = stat.executeQuery("SELECT COUNT(*) FROM (SELECT CaseID FROM " + tableName + " GROUP BY CaseID)");
		rs.next();
		System.out.println("\tnumber of cases:\t" + rs.getInt(1));

		rs = stat.executeQuery("SELECT COUNT(*) FROM (SELECT Activity FROM " + tableName + " GROUP BY Activity)");
		rs.next();
		System.out.println("\tnumber of event types:\t" + rs.getInt(1));
		
		rs.close();
	}
	
	public static void main(String args[]) throws ClassNotFoundException, SQLException {
		/*
		SandboxGeneration test = new SandboxGeneration();

		test.loadLog("appeals");
		test.printTableSize("appeals");
		test.close();
		*/
		MarkovGeneration mc = new MarkovGeneration("./tempdb","appeals");
		//System.out.print(mc.toString());
		System.out.println(mc.generateSequence(0.0, "1"));
		
	}
	
}
