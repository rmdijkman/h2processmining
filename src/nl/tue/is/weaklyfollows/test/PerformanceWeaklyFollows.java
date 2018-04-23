package nl.tue.is.weaklyfollows.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class PerformanceWeaklyFollows {

	Connection conn;
	Statement stat;
	long startTime;

	public PerformanceWeaklyFollows() throws ClassNotFoundException, SQLException{
		Class.forName("org.h2.Driver");
		conn = DriverManager.getConnection("jdbc:h2:mem:", "sa", "");
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

	public void printTableDefinition(String tableName) throws SQLException{
		ResultSet rs = stat.executeQuery("SELECT * FROM " + tableName);
		System.out.println(tableName + "(");
		for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++){
			System.out.println("\t" + rs.getMetaData().getColumnName(i) + "\t" + rs.getMetaData().getColumnTypeName(i));
		}
		System.out.println(")");
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
	}

	public void printResultSet(ResultSet rs) throws SQLException{
		while (rs.next()){
			StringBuilder sb = new StringBuilder();
			ResultSetMetaData rsmd = rs.getMetaData();
			int numberOfColumns = rsmd.getColumnCount();
			for (int i = 1; i <= numberOfColumns; i++) {
				sb.append(rs.getString(i));
				if (i < numberOfColumns) {
					sb.append(", ");
				}
			}
			String data = sb.toString();
			System.out.println(data);
		}
	}public ResultSet executeQuery(String query) throws SQLException{
		return stat.executeQuery(query);
	}
	
	public void startTimeMeasurement(){
		startTime = System.currentTimeMillis();
	}
	
	public void printTimeTaken(){
		long endTime = System.currentTimeMillis();
		System.out.println("\ttime taken: " + (endTime - startTime) + " ms.");
	}
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		if (args.length < 1){
			System.out.println("Provide the event log that must be loaded as an argument, e.g.: BPI2011.");
			System.exit(0);
		}
		String logName = args[0];
		
		PerformanceWeaklyFollows test = new PerformanceWeaklyFollows();

		test.loadLog(logName);
		test.printTableSize(logName);

		System.out.println("Measuring the time taken to execute the weakly follows relation on the "+logName+" log ...");
		test.startTimeMeasurement();
		test.executeQuery("SELECT * FROM FOLLOWS(SELECT caseid,activity,completetimestamp FROM " + logName + ")");
		test.printTimeTaken();

		System.out.println("Measuring the time taken to execute a nested query to get the weakly follows relation on the "+logName+" log ...");
		test.startTimeMeasurement();
		test.executeQuery(
				"  SELECT DISTINCT a.Activity, b.Activity "
				+ "FROM "+logName+" a, "+logName+" b "
				+ "WHERE a.CaseID = b.CaseID AND a.CompleteTimestamp < b.CompleteTimestamp AND "
				+ "  NOT EXISTS("
				+ "    SELECT * "
				+ "    FROM "+logName+" c "
				+ "    WHERE c.CaseID = a.CaseID AND a.CompleteTimestamp < c.CompleteTimestamp AND c.CompleteTimestamp < b.CompleteTimestamp"
				+ "  );");
		test.printTimeTaken();		

		test.close();
	}

}
