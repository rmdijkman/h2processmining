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
		conn = DriverManager.getConnection("jdbc:h2:~/performancedb;CACHE_SIZE=8388608;PAGE_SIZE=512", "sa", "");
		stat = conn.createStatement();
	}
	
	public void close() throws SQLException{
		stat.close();
		conn.close();
	}
	
	public void loadBPI2012() throws SQLException{
		stat.execute("CREATE TABLE IF NOT EXISTS BPI2012 "
				+ "AS SELECT "
				+ "CaseID,"
				+ "Activity,"
				+ "Resource,"
				+ "convert(parseDateTime(CompleteTimestamp,'yyyy/MM/dd hh:mm:ss'),TIMESTAMP) AS CompleteTimestamp,"
				+ "Variant,"
				+ "VariantIndex,"
				+ "AmountReq,"
				+ "Name,"
				+ "LifecycleTransition "
				+ "FROM CSVREAD('./resources/BPI2012.csv', null, 'fieldSeparator=;');");

		stat.execute("CREATE TABLE IF NOT EXISTS BPI2012INDEXED "
				+ "AS SELECT "
				+ "CaseID,"
				+ "Activity,"
				+ "Resource,"
				+ "convert(parseDateTime(CompleteTimestamp,'yyyy/MM/dd hh:mm:ss'),TIMESTAMP) AS CompleteTimestamp,"
				+ "Variant,"
				+ "VariantIndex,"
				+ "AmountReq,"
				+ "Name,"
				+ "LifecycleTransition "
				+ "FROM CSVREAD('./resources/BPI2012.csv', null, 'fieldSeparator=;');");
		stat.execute("CREATE INDEX IF NOT EXISTS CaseID_idx ON BPI2012INDEXED(CaseID)");
		stat.execute("CREATE INDEX IF NOT EXISTS Activity_idx ON BPI2012INDEXED(Activity)");
		stat.execute("CREATE INDEX IF NOT EXISTS CompleteTimestamp_idx ON BPI2012INDEXED(CompleteTimestamp)");
	}

	public void loadBPI2011() throws SQLException{
		stat.execute("CREATE TABLE IF NOT EXISTS BPI2011 "
				+ "AS SELECT "
				+ "CaseID,"
				+ "Activity,"
				+ "convert(parseDateTime(CompleteTimestamp,'yyyy/MM/dd hh:mm:ss'),TIMESTAMP) AS CompleteTimestamp "
				+ "FROM CSVREAD('./resources/BPI2011.csv', null, 'fieldSeparator=;');");
		
		stat.execute("CREATE TABLE IF NOT EXISTS BPI2011INDEXED "
				+ "AS SELECT "
				+ "CaseID,"
				+ "Activity,"
				+ "convert(parseDateTime(CompleteTimestamp,'yyyy/MM/dd hh:mm:ss'),TIMESTAMP) AS CompleteTimestamp "
				+ "FROM CSVREAD('./resources/BPI2011.csv', null, 'fieldSeparator=;');");
		stat.execute("CREATE INDEX IF NOT EXISTS CaseID_idx ON BPI2011INDEXED(CaseID)");
		stat.execute("CREATE INDEX IF NOT EXISTS Activity_idx ON BPI2011INDEXED(Activity)");
		stat.execute("CREATE INDEX IF NOT EXISTS CompleteTimestamp_idx ON BPI2011INDEXED(CompleteTimestamp)");
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
		PerformanceWeaklyFollows test = new PerformanceWeaklyFollows();

		test.printResultSet(test.executeQuery("SELECT * FROM INFORMATION_SCHEMA.SETTINGS WHERE NAME = 'info.CACHE_MAX_SIZE'"));
		
		test.loadBPI2011();
		test.printTableSize("BPI2011INDEXED");
		test.loadBPI2012();
		test.printTableSize("BPI2012INDEXED");

		//test.printTableDefinition("BPI2012");		
		//test.printResultSet(test.executeQuery("SELECT * FROM BPI2012"));

		System.out.println("Measuring the time taken to execute the weakly follows relation on the indexed indexed BPI 2012  log ...");
		test.startTimeMeasurement();
		test.executeQuery("SELECT * FROM FOLLOWS(SELECT caseid,activity,completetimestamp FROM BPI2012INDEXED)");
		test.printTimeTaken();

		System.out.println("Measuring the time taken to execute a nested query to get the weakly follows relation on the indexed BPI 2012 log ...");
		test.startTimeMeasurement();
		test.executeQuery(
				"  SELECT DISTINCT a.Activity, b.Activity "
				+ "FROM BPI2012INDEXED a, BPI2012INDEXED b "
				+ "WHERE a.CaseID = b.CaseID AND a.CompleteTimestamp < b.CompleteTimestamp AND "
				+ "  NOT EXISTS("
				+ "    SELECT * "
				+ "    FROM BPI2012INDEXED c "
				+ "    WHERE c.CaseID = a.CaseID AND a.CompleteTimestamp < c.CompleteTimestamp AND c.CompleteTimestamp < b.CompleteTimestamp"
				+ "  );");
		test.printTimeTaken();		

		System.out.println("Measuring the time taken to execute the weakly follows relation on the indexed BPI 2011 log ...");
		test.startTimeMeasurement();
		test.executeQuery("SELECT * FROM FOLLOWS(SELECT caseid,activity,completetimestamp FROM BPI2011INDEXED)");
		test.printTimeTaken();
		
		System.out.println("Measuring the time taken to execute a nested query to get the weakly follows relation on the indexed BPI 2011 log ...");
		test.startTimeMeasurement();
		test.executeQuery(
				"  SELECT DISTINCT a.Activity, b.Activity "
				+ "FROM BPI2011INDEXED a, BPI2011INDEXED b "
				+ "WHERE a.CaseID = b.CaseID AND a.CompleteTimestamp < b.CompleteTimestamp AND "
				+ "  NOT EXISTS("
				+ "    SELECT * "
				+ "    FROM BPI2011INDEXED c "
				+ "    WHERE c.CaseID = a.CaseID AND a.CompleteTimestamp < c.CompleteTimestamp AND c.CompleteTimestamp < b.CompleteTimestamp"
				+ "  );");
		test.printTimeTaken();		

		System.out.println("Measuring the time taken to execute a nested query to get the weakly follows relation on the BPI 2012 log ...");
		test.startTimeMeasurement();
		test.executeQuery(
				"  SELECT DISTINCT a.Activity, b.Activity "
				+ "FROM BPI2012 a, BPI2012 b "
				+ "WHERE a.CaseID = b.CaseID AND a.CompleteTimestamp < b.CompleteTimestamp AND "
				+ "  NOT EXISTS("
				+ "    SELECT * "
				+ "    FROM BPI2012 c "
				+ "    WHERE c.CaseID = a.CaseID AND a.CompleteTimestamp < c.CompleteTimestamp AND c.CompleteTimestamp < b.CompleteTimestamp"
				+ "  );");
		test.printTimeTaken();		

		System.out.println("Measuring the time taken to execute a nested query to get the weakly follows relation on the BPI 2011 log ...");
		test.startTimeMeasurement();
		test.executeQuery(
				"  SELECT DISTINCT a.Activity, b.Activity "
				+ "FROM BPI2011 a, BPI2011 b "
				+ "WHERE a.CaseID = b.CaseID AND a.CompleteTimestamp < b.CompleteTimestamp AND "
				+ "  NOT EXISTS("
				+ "    SELECT * "
				+ "    FROM BPI2011 c "
				+ "    WHERE c.CaseID = a.CaseID AND a.CompleteTimestamp < c.CompleteTimestamp AND c.CompleteTimestamp < b.CompleteTimestamp"
				+ "  );");
		test.printTimeTaken();		

		test.close();
	}

}
