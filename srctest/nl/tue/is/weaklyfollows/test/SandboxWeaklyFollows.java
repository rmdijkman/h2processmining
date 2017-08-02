package nl.tue.is.weaklyfollows.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class SandboxWeaklyFollows {

	public static void main(String[] args) throws Exception{
		Class.forName("org.h2.Driver");
		Connection conn = DriverManager.getConnection("jdbc:h2:mem:", "sa", "");
		Statement stat = conn.createStatement();
		ResultSet rs;

		//Create the event log
		stat.execute("CREATE TABLE Event_Log(Case_Id INT, Event VARCHAR(100), End_Time TIME)");
		stat.execute("INSERT INTO Event_Log VALUES (1,'A','00:22:00')");
		stat.execute("INSERT INTO Event_Log VALUES (1,'B','02:08:00')");
		stat.execute("INSERT INTO Event_Log VALUES (1,'E','02:32:00')");
		stat.execute("INSERT INTO Event_Log VALUES (2,'A','02:20:00')");
		stat.execute("INSERT INTO Event_Log VALUES (2,'D','03:19:00')");
		stat.execute("INSERT INTO Event_Log VALUES (2,'E','05:07:00')");
		stat.execute("INSERT INTO Event_Log VALUES (3,'A','02:29:00')");
		stat.execute("INSERT INTO Event_Log VALUES (3,'D','04:20:00')");
		stat.execute("INSERT INTO Event_Log VALUES (3,'E','06:53:00')");
		stat.execute("INSERT INTO Event_Log VALUES (4,'A','03:10:00')");
		stat.execute("INSERT INTO Event_Log VALUES (4,'B','05:09:00')");
		stat.execute("INSERT INTO Event_Log VALUES (4,'E','07:29:00')");
		stat.execute("INSERT INTO Event_Log VALUES (5,'A','03:44:00')");
		stat.execute("INSERT INTO Event_Log VALUES (5,'B','06:06:00')");
		stat.execute("INSERT INTO Event_Log VALUES (5,'E','07:52:00')");
		stat.execute("INSERT INTO Event_Log VALUES (6,'A','04:20:00')");
		stat.execute("INSERT INTO Event_Log VALUES (6,'C','07:12:00')");
		stat.execute("INSERT INTO Event_Log VALUES (6,'E','09:07:00')");

		//Query and print the weakly follows relation
		rs = stat.executeQuery("SELECT * FROM FOLLOWS(SELECT * FROM Event_Log)");
		while (rs.next()) {
			System.out.println(rs.getString(1) + "\t" + rs.getString(2));
		}        

		stat.close();
		conn.close();
	}

}
