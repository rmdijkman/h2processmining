package nl.tue.is.weaklyfollows.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import nl.tue.util.StringPadding;

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
	
	public void loadLog(String folder, String logName) throws SQLException{
		stat.execute("CREATE TABLE IF NOT EXISTS " + logName
				+ " AS SELECT "
				+ "CaseID,"
				+ "Activity,"
				+ "convert(parseDateTime(CompleteTimestamp,'yyyy/MM/dd hh:mm:ss'),TIMESTAMP) AS CompleteTimestamp "
				+ "FROM CSVREAD('"+folder+logName+".csv', null, 'fieldSeparator=;');");
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
	
	public void createOrderedLog(String logName) throws SQLException{
		stat.execute("CREATE TABLE IF NOT EXISTS " + logName + "ORDERED ("
				+ "Seqnr INTEGER PRIMARY KEY AUTO_INCREMENT"
				+ ", CaseID VARCHAR"
				+ ", Activity VARCHAR"
				+ ", CompleteTimestamp TIMESTAMP"
				+ ")");
		stat.execute("CREATE INDEX IF NOT EXISTS CaseID_idx ON "+logName+"ORDERED(CaseID)");
		stat.execute("CREATE INDEX IF NOT EXISTS Activity_idx ON "+logName+"ORDERED(Activity)");
		stat.execute("CREATE INDEX IF NOT EXISTS CompleteTimestamp_idx ON "+logName+"ORDERED(CompleteTimestamp)");
		
		stat.execute("INSERT INTO " + logName + "ORDERED (CaseId,Activity,CompleteTimestamp) SELECT * FROM " + logName + " ORDER BY CaseId, CompleteTimestamp");
		
		//Execute a simple query first, because otherwise the indexes do not seem to be initialized, 
		//which causes a very slow response in the nested query.
		stat.executeQuery(
				"  SELECT DISTINCT a.Activity, b.Activity "
				+ "FROM "+logName+"ORDERED a, "+logName+"ORDERED b "
				+ "WHERE a.CaseID = b.CaseID AND a.CompleteTimestamp < b.CompleteTimestamp AND a.Seqnr = b.Seqnr - 1"
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

	public int nrEvents(String tableName) throws SQLException{		
		ResultSet rs = stat.executeQuery("SELECT COUNT(*) FROM " + tableName);
		rs.next();
		return rs.getInt(1);
	}
	
	public int nrCases(String tableName) throws SQLException{		
		ResultSet rs = stat.executeQuery("SELECT COUNT(*) FROM (SELECT CaseID FROM " + tableName + " GROUP BY CaseID)");
		rs.next();
		return rs.getInt(1);
	}
	
	public int nrEventTypes(String tableName) throws SQLException{		
		ResultSet rs = stat.executeQuery("SELECT COUNT(*) FROM (SELECT Activity FROM " + tableName + " GROUP BY Activity)");
		rs.next();
		return rs.getInt(1);
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
	}
	
	public ResultSet executeQuery(String query) throws SQLException{
		return stat.executeQuery(query);
	}
	
	public void startTimeMeasurement(){
		startTime = System.currentTimeMillis();
	}
	
	public long timeTaken(){
		long endTime = System.currentTimeMillis();
		return (endTime - startTime); //in milliseconds
	}
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		if (args.length < 2){
			System.out.println("Provide the folder from which the event logs must be loaded (include trailing slash) "
							+ "and the event logs that must be loaded as a arguments, e.g.: ./resources/ BPI2011 BPI2012.");
			System.exit(0);
		}

		System.out.println(
				"log name" + "\t" + 
				"#cases" + "\t" + 
				"#events" + "\t" + 
				"#event types" + "\t" + 
				"native(ms)" + "\t" + 
				"sorted(ms)" + "\t" + 
				"nested(ms)" + "\t" +
				"actual#rels" + "\t" +				
				"sorted#rels"
			);
		
		String folder = args[0];
		
		for (int i = 1; i < args.length; i++) {
			String logName = args[i];
		
			PerformanceWeaklyFollows test = new PerformanceWeaklyFollows();

			test.loadLog(folder, logName);
			test.createOrderedLog(logName);

			//Native
			test.startTimeMeasurement();
			ResultSet rsNative = test.executeQuery("SELECT * FROM FOLLOWS(SELECT caseid,activity,completetimestamp FROM " + logName + ")");
			long nativeTime = test.timeTaken();
			long actualRels = 0;
			if (rsNative.last()) {
				actualRels = rsNative.getRow();
			}
			
			//Nested
			test.startTimeMeasurement();			
			ResultSet rsNested = test.executeQuery(
				"  SELECT DISTINCT a.Activity, b.Activity "
				+ "FROM "+logName+" a, "+logName+" b "
				+ "WHERE a.CaseID = b.CaseID AND a.CompleteTimestamp < b.CompleteTimestamp AND "
				+ "  NOT EXISTS("
				+ "    SELECT * "
				+ "    FROM "+logName+" c "
				+ "    WHERE c.CaseID = a.CaseID AND a.CompleteTimestamp < c.CompleteTimestamp AND c.CompleteTimestamp < b.CompleteTimestamp"
				+ "  );");			
			long nestedTime = test.timeTaken();

			//Sorted
			test.startTimeMeasurement();
			ResultSet rsSorted = test.executeQuery(
				"  SELECT DISTINCT a.Activity, b.Activity "
				+ "FROM "+logName+"ORDERED a, "+logName+"ORDERED b "
				+ "WHERE a.CaseID = b.CaseID AND a.CompleteTimestamp < b.CompleteTimestamp AND b.Seqnr = a.Seqnr + 1");
			long sortedTime = test.timeTaken();
			long sortedRels = 0;
			if (rsSorted.last()) {
				sortedRels = rsSorted.getRow();
			}

			System.out.println(
					logName + "\t" +
					test.nrCases(logName) + "\t" + 
					test.nrEvents(logName) + "\t" + 
					test.nrEventTypes(logName) + "\t" + 
					nativeTime + "\t" + 
					sortedTime + "\t" +
					nestedTime + "\t" +
					actualRels + "\t" +
					sortedRels
				);
			
			test.close();
		}
	}

}
