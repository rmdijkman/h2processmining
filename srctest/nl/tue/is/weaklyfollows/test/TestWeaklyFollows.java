package nl.tue.is.weaklyfollows.test;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestWeaklyFollows {

	static Connection conn;
	static Statement stat;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		Class.forName("org.h2.Driver");
		conn = DriverManager.getConnection("jdbc:h2:mem:", "sa", "");
		stat = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY );
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		stat.close();
		conn.close();
	}

	private boolean resultSetContains(ResultSet rs, String antecedent, String consequent) throws SQLException{
		rs.beforeFirst();
		while (rs.next()){
			if (rs.getString(1).equals(antecedent) && rs.getString(2).equals(consequent)){
				return true;
			}
		}
		return false;
	}

	private int resultSetSize(ResultSet rs) throws SQLException{
		rs.last();
		return rs.getRow();
	}
	
	/*
	 * Test a single case sequence. 
	 */
	@Test
	public void testA() throws SQLException {
		stat.execute("CREATE TABLE TestA(Case_Id INT, Event VARCHAR(100), End_Time TIME)");
		stat.execute("INSERT INTO TestA VALUES (1,'A','01:00:00')");
		stat.execute("INSERT INTO TestA VALUES (1,'B','02:00:00')");
		stat.execute("INSERT INTO TestA VALUES (1,'C','03:00:00')");
		
		ResultSet rs = stat.executeQuery("SELECT * FROM FOLLOWS(SELECT * FROM TestA)");
		
		assertTrue("The result should be {(A,B),(B,C)}", resultSetSize(rs) == 2);
		assertTrue("The result should be {(A,B),(B,C)}", resultSetContains(rs,"A","B"));
		assertTrue("The result should be {(A,B),(B,C)}", resultSetContains(rs,"B","C"));
	}

	/*
	 * Test two case sequences with overlapping event labels and time stamps. 
	 */
	@Test
	public void testB() throws SQLException{
		stat.execute("CREATE TABLE TestB(Case_Id INT, Event VARCHAR(100), End_Time TIME)");
		stat.execute("INSERT INTO TestB VALUES (1,'A','01:00:00')");
		stat.execute("INSERT INTO TestB VALUES (2,'A','01:00:00')");
		stat.execute("INSERT INTO TestB VALUES (1,'B','02:00:00')");
		stat.execute("INSERT INTO TestB VALUES (2,'B','02:00:00')");
		stat.execute("INSERT INTO TestB VALUES (1,'C','03:00:00')");
		stat.execute("INSERT INTO TestB VALUES (2,'D','03:00:00')");
		
		ResultSet rs = stat.executeQuery("SELECT * FROM FOLLOWS(SELECT * FROM TestB)");
		
		assertTrue("The result should be {(A,B),(B,C),(B,D)}", resultSetSize(rs) == 3);
		assertTrue("The result should be {(A,B),(B,C),(B,D)}", resultSetContains(rs,"A","B"));
		assertTrue("The result should be {(A,B),(B,C),(B,D)}", resultSetContains(rs,"B","C"));
		assertTrue("The result should be {(A,B),(B,C),(B,D)}", resultSetContains(rs,"B","D"));
	}

	/*
	 * Test a single case sequence with antecedents and consequents that happen at the same time. 
	 */
	@Test
	public void testC() throws SQLException {
		stat.execute("CREATE TABLE testC(Case_Id INT, Event VARCHAR(100), End_Time TIME)");
		stat.execute("INSERT INTO testC VALUES (1,'A','01:00:00')");
		stat.execute("INSERT INTO testC VALUES (1,'B','02:00:00')");
		stat.execute("INSERT INTO testC VALUES (1,'C','02:00:00')");
		stat.execute("INSERT INTO testC VALUES (1,'D','03:00:00')");
		stat.execute("INSERT INTO testC VALUES (1,'E','03:00:00')");
		stat.execute("INSERT INTO testC VALUES (1,'F','04:00:00')");

		ResultSet rs = stat.executeQuery("SELECT * FROM FOLLOWS(SELECT * FROM testC)");
		
		assertTrue("The result should be {(A,B),(A,C),(B,D),(B,E),(C,D),(C,E),(D,F),(E,F)}", resultSetSize(rs) == 8);
		assertTrue("The result should be {(A,B),(A,C),(B,D),(B,E),(C,D),(C,E),(D,F),(E,F)}", resultSetContains(rs,"A","B"));
		assertTrue("The result should be {(A,B),(A,C),(B,D),(B,E),(C,D),(C,E),(D,F),(E,F)}", resultSetContains(rs,"A","C"));
		assertTrue("The result should be {(A,B),(A,C),(B,D),(B,E),(C,D),(C,E),(D,F),(E,F)}", resultSetContains(rs,"B","D"));
		assertTrue("The result should be {(A,B),(A,C),(B,D),(B,E),(C,D),(C,E),(D,F),(E,F)}", resultSetContains(rs,"B","E"));
		assertTrue("The result should be {(A,B),(A,C),(B,D),(B,E),(C,D),(C,E),(D,F),(E,F)}", resultSetContains(rs,"C","D"));
		assertTrue("The result should be {(A,B),(A,C),(B,D),(B,E),(C,D),(C,E),(D,F),(E,F)}", resultSetContains(rs,"C","E"));
		assertTrue("The result should be {(A,B),(A,C),(B,D),(B,E),(C,D),(C,E),(D,F),(E,F)}", resultSetContains(rs,"D","F"));
		assertTrue("The result should be {(A,B),(A,C),(B,D),(B,E),(C,D),(C,E),(D,F),(E,F)}", resultSetContains(rs,"E","F"));
	}

	/*
	 * Test a single case sequence with antecedents and consequents at the start and at the end of the sequence. 
	 */
	@Test
	public void testD() throws SQLException {
		stat.execute("CREATE TABLE testD(Case_Id INT, Event VARCHAR(100), End_Time TIME)");
		stat.execute("INSERT INTO testD VALUES (1,'A','01:00:00')");
		stat.execute("INSERT INTO testD VALUES (1,'B','01:00:00')");
		stat.execute("INSERT INTO testD VALUES (1,'C','02:00:00')");
		stat.execute("INSERT INTO testD VALUES (1,'D','03:00:00')");
		stat.execute("INSERT INTO testD VALUES (1,'E','03:00:00')");

		ResultSet rs = stat.executeQuery("SELECT * FROM FOLLOWS(SELECT * FROM testD)");
		
		assertTrue("The result should be {(A,C),(B,C),(C,D),(C,E)}", resultSetSize(rs) == 4);
		assertTrue("The result should be {(A,B),(B,C),(C,D),(C,E)}", resultSetContains(rs,"A","C"));
		assertTrue("The result should be {(A,B),(B,C),(C,D),(C,E)}", resultSetContains(rs,"B","C"));
		assertTrue("The result should be {(A,B),(B,C),(C,D),(C,E)}", resultSetContains(rs,"C","D"));
		assertTrue("The result should be {(A,B),(B,C),(C,D),(C,E)}", resultSetContains(rs,"C","E"));
	}

	/*
	 * Test a single case sequence with antecedents and consequents that have the same label. 
	 */
	@Test
	public void testE() throws SQLException {
		stat.execute("CREATE TABLE testE(Case_Id INT, Event VARCHAR(100), End_Time TIME)");
		stat.execute("INSERT INTO testE VALUES (1,'A','01:00:00')");
		stat.execute("INSERT INTO testE VALUES (1,'B','02:00:00')");
		stat.execute("INSERT INTO testE VALUES (1,'B','02:00:00')");
		stat.execute("INSERT INTO testE VALUES (1,'C','03:00:00')");

		ResultSet rs = stat.executeQuery("SELECT * FROM FOLLOWS(SELECT * FROM testE)");
		
		assertTrue("The result should be {(A,B),(B,C)}", resultSetSize(rs) == 2);
		assertTrue("The result should be {(A,B),(B,C)}", resultSetContains(rs,"A","B"));
		assertTrue("The result should be {(A,B),(B,C)}", resultSetContains(rs,"B","C"));
	}
	
	/*
	 * Test the toy example from the paper. 
	 */
	@Test
	public void testF() throws SQLException {
		//Create the event log
		stat.execute("CREATE TABLE testF(Case_Id INT, Event VARCHAR(100), End_Time TIME)");
		stat.execute("INSERT INTO testF VALUES (1,'A','00:22:00')");
		stat.execute("INSERT INTO testF VALUES (1,'B','02:08:00')");
		stat.execute("INSERT INTO testF VALUES (1,'E','02:32:00')");
		stat.execute("INSERT INTO testF VALUES (2,'A','02:20:00')");
		stat.execute("INSERT INTO testF VALUES (2,'D','03:19:00')");
		stat.execute("INSERT INTO testF VALUES (2,'E','05:07:00')");
		stat.execute("INSERT INTO testF VALUES (3,'A','02:29:00')");
		stat.execute("INSERT INTO testF VALUES (3,'D','04:20:00')");
		stat.execute("INSERT INTO testF VALUES (3,'E','06:53:00')");
		stat.execute("INSERT INTO testF VALUES (4,'A','03:10:00')");
		stat.execute("INSERT INTO testF VALUES (4,'B','05:09:00')");
		stat.execute("INSERT INTO testF VALUES (4,'E','07:29:00')");
		stat.execute("INSERT INTO testF VALUES (5,'A','03:44:00')");
		stat.execute("INSERT INTO testF VALUES (5,'B','06:06:00')");
		stat.execute("INSERT INTO testF VALUES (5,'E','07:52:00')");
		stat.execute("INSERT INTO testF VALUES (6,'A','04:20:00')");
		stat.execute("INSERT INTO testF VALUES (6,'C','07:12:00')");
		stat.execute("INSERT INTO testF VALUES (6,'E','09:07:00')");

		ResultSet rs = stat.executeQuery("SELECT * FROM FOLLOWS(SELECT * FROM testF)");
		
		assertTrue("The result should be {(A,B),(A,C),(A,D),(B,E),(C,E),(D,E)}", resultSetSize(rs) == 6);
		assertTrue("The result should be {(A,B),(A,C),(A,D),(B,E),(C,E),(D,E)}", resultSetContains(rs,"A","B"));
		assertTrue("The result should be {(A,B),(A,C),(A,D),(B,E),(C,E),(D,E)}", resultSetContains(rs,"A","C"));
		assertTrue("The result should be {(A,B),(A,C),(A,D),(B,E),(C,E),(D,E)}", resultSetContains(rs,"A","D"));
		assertTrue("The result should be {(A,B),(A,C),(A,D),(B,E),(C,E),(D,E)}", resultSetContains(rs,"B","E"));
		assertTrue("The result should be {(A,B),(A,C),(A,D),(B,E),(C,E),(D,E)}", resultSetContains(rs,"C","E"));
		assertTrue("The result should be {(A,B),(A,C),(A,D),(B,E),(C,E),(D,E)}", resultSetContains(rs,"D","E"));
	}

	/*
	 * Test empty log. 
	 */
	@Test
	public void testG() throws SQLException {
		stat.execute("CREATE TABLE testG(Case_Id INT, Event VARCHAR(100), End_Time TIME)");
		
		ResultSet rs = stat.executeQuery("SELECT * FROM FOLLOWS(SELECT * FROM testG)");
		
		assertTrue("The result should be {}", resultSetSize(rs) == 0);
	}

	/*
	 * Test case without weakly follows relations. 
	 */
	@Test
	public void testH() throws SQLException {
		stat.execute("CREATE TABLE testH(Case_Id INT, Event VARCHAR(100), End_Time TIME)");
		stat.execute("INSERT INTO testH VALUES (1,'A','01:00:00')");
		
		ResultSet rs = stat.executeQuery("SELECT * FROM FOLLOWS(SELECT * FROM testH)");
		
		assertTrue("The result should be {}", resultSetSize(rs) == 0);
	}
}
