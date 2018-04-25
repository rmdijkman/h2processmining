package nl.tue.is.weaklyfollows.test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import nl.tue.loggeneration.LogGenerationException;
import nl.tue.loggeneration.MarkovGeneration;

public class SandboxGeneration {

	Connection conn;
	Statement stat;
	long startTime;

	public SandboxGeneration() throws ClassNotFoundException, SQLException{
		Class.forName("org.h2.Driver");
		conn = DriverManager.getConnection("jdbc:h2:./temp/tempdb", "sa", "");
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
	
	public static void generateCollectionByExtension() throws ClassNotFoundException, SQLException, FileNotFoundException {
		System.out.println("Loading Log.");
		
		SandboxGeneration test = new SandboxGeneration();
		test.loadLog("BPI2012");
		test.close();

		System.out.println("Initializing generator.");		
		
		MarkovGeneration mc = new MarkovGeneration("./temp/tempdb","BPI2012");
		
		System.out.println("Done initializing.");
		
		/* 20 is the expected number of events per case in the BPI2012 log.
		 * 130 is the expected number of events per case in the BPI2011 log.
		 * For this experiment, we will extend the expected number of events:
		 * - for the BPI2012 log
		 * - in steps of 10 (i.e. a factor 1.5, 2, 2.5, ...)
		 * - until we reach a factor 5 (i.e. 100 expected events per case)
		 */
		for (double factor = 1.0; factor < 5.2; factor += 0.5) {
			boolean success = false;
			while (!success) {
				try {
					System.out.println("Generating log for factor " + factor);
					MarkovGeneration mcexperiment = mc.clone();
					mcexperiment.extendByExpectedExecutions(factor);
					String stringfactor = Double.toString(Math.round(factor*10.0));
					mc.generateLog(1000, "./temp/experiment_1_"+stringfactor+".csv");
					success = true;
				} catch (LogGenerationException e) {
					System.out.println(e.getMessage());
					System.out.println("Retrying");
				}
			}
		}
		
	}

	public static void generateCollectionByReduction() throws ClassNotFoundException, SQLException, LogGenerationException, IOException {
		
		System.out.println("Loading Log.");		
		
		SandboxGeneration test = new SandboxGeneration();
		test.loadLog("BPI2011");
		test.close();

		System.out.println("Initializing generator.");		
		
		MarkovGeneration mc = new MarkovGeneration("./temp/tempdb","BPI2011");
		//mc.serialize("./temp/BPI2011Generator.ser");
		
		//MarkovGeneration mc = MarkovGeneration.deserialize("./temp/BPI2011Generator.ser");
		System.out.println("Done initializing.");		
		
		/* 130 is the expected number of events per case in the BPI2011 log.
		 * For this experiment, we will reduce the expected number of events:
		 * - for the BPI2011 log
		 * - in steps of 5 (i.e. a factor 12.5/13, 11.5/13, 10.5/13, ...)
		 * - until we reach a factor 0.24 (below that, the log becomes empty)
		 */
		for (double factor = (13.0d/13.0d); factor > 0.23; factor -= (0.5d/13.0d)) {
			MarkovGeneration mcexperiment = mc.clone();			
			System.out.print("Creating log reduced by " + factor + " ... ");
			double actualfactor = mcexperiment.reduceByExpectedExecutions(factor);
			String stringfactor = Integer.toString((int) Math.round(actualfactor*100.0));
			System.out.println("realized reduction: " + actualfactor);
			mcexperiment.generateLog(1000, "./temp/experiment_1_"+stringfactor+".csv");
		}
		
		/*
		 * We'll also extend the log in steps of 10
		 * in steps of 5 (i.e. a factor 13.5/13, 14/14)
		 * until we reach 15/13
		 */
		for (double factor = (13.0d/13.0d); factor < 15.25d/13.0d; factor += (0.5d/13.0d)) {
			MarkovGeneration mcexperiment = mc.clone();			
			System.out.print("Creating log extended by " + factor + " ... ");
			double actualfactor = mcexperiment.extendByExpectedExecutions(factor);
			String stringfactor = Integer.toString((int) Math.round(actualfactor*100.0));
			System.out.println("realized extension: " + actualfactor);
			mcexperiment.generateLog(1000, "./temp/experiment_1_"+stringfactor+".csv");
		}		
	}
	
	public static void main(String args[]) throws ClassNotFoundException, SQLException, LogGenerationException, IOException {
		/*
		SandboxGeneration test = new SandboxGeneration();

		test.loadLog("testlog");
		test.printTableSize("testlog");
		test.close();
		*/
		//MarkovGeneration mc = new MarkovGeneration("./temp/tempdb","testlog");
		//mc.serialize("./temp/generator.ser");
		//System.out.println(MarkovGeneration.stateProbabilityToString(mc.expectedExecutions()));
		//System.out.println(mc.totalExpectedExecutions());
		
		//mc.addActivity("ADD");
		//System.out.println(mc.toString());
		//mc.removeActivity();
		//mc.generateLog(100, "testoriginal.csv");
		
		//mc.generateLog(100, "./temp/testoriginal.csv");
		//mc.extendByExpectedExecutions(5.0/2.2);
		//mc.generateLog(100, "./temp/testextended.csv");

		//MarkovGeneration mcp = MarkovGeneration.deserialize("./temp/generator.ser");
		//System.out.println(mcp.toString());
		//mc.reduceByExpectedExecutions(0.9);
		//System.out.println(mc.toString());
		//mcp.generateLog(100, "./temp/testreduced.csv");
		
		generateCollectionByReduction();
	}
	
}
