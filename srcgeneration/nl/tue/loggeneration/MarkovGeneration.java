package nl.tue.loggeneration;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Enables log generation based on a Markov chain.
 * The Markov chain is initialized based on an existing log. It describes:
 * 1. the probability that a certain action follows another action 
 * 2. the average time till the completion of an action from the completion of the previous action
 * 3. the interarrival time of cases
 * It will generate a log that has average interarrival times between cases,
 * contains cases that have a sequence that is generated based on the probability of actions following other actions,
 * and average time between the completion of actions.
 * This can easily be improved by also generating interarrival times and completion times based on averages.
 * However, for the use-case in this project that is not necessary.
 * The generator generates a log with case identifiers, activity labels and completion times.
 * The generated log will - when process mined - on average produce a process model that is similar to the model that can be mined from the original log.  
 */
public class MarkovGeneration {

	/*
	 * The actual Markov chain.
	 * The value chain[i][j]
	 * = the probability that j is the first symbol in a sequence, if i == 0
	 * = the probability that j follows i in a sequence, if i > 0 AND j > 0
	 * = the probability that i is the last symbol in a sequence, if i > 0 AND j == 0
	 * The value timing[i][j] (in seconds)
	 * = the average time it takes until the first symbol j completes in a sequence, if i == 0
	 * = the average time it takes until the symbol j completed in a sequence after i, if i > 0 AND j > 0
	 * = 0, if i > 0 AND j == 0
	 */
	private double[][] chain;
	private double[][] timing;
	
	/*
	 * Used to transform activity labels to numbers (and back) for efficiency.
	 */
	private int nextId;
	private Map<String,Integer> activity2Id;
	private Map<Integer,String> id2Activity;

	private MarkovGeneration(){		
		nextId = 1;
		activity2Id = new HashMap<String,Integer>();
		id2Activity = new HashMap<Integer,String>();
	}

	/**
	 * Computes the Markov chain for the given database table.
	 * The database must be H2. The table must have the columns CaseID, Activity, CompleteTimestamp
	 * 
	 * @param dbFile the file in which the database is stored.
	 * @param tableName the name of the table in the database.
	 */
	public MarkovGeneration(String dbFile, String tableName) throws SQLException, ClassNotFoundException{
		this();
		
		Class.forName("org.h2.Driver");
		Connection conn = DriverManager.getConnection("jdbc:h2:" + dbFile, "sa", "");
		Statement stat = conn.createStatement();
		
		ResultSet rs = stat.executeQuery("SELECT CaseID,Activity,CompleteTimestamp FROM " + tableName + " ORDER BY CaseID,CompleteTimestamp");
		String currentID = "";
		List<List<Integer>> sequences = new LinkedList<List<Integer>>();
		List<List<Double>> times = new LinkedList<List<Double>>();
		List<Integer> currentSequence = null;
		List<Double> currentTimes = new LinkedList<Double>();
		Double previousStartTime = 0.0;
		Double previousCompletionTime = 0.0;
		while (rs.next()) {
			String caseID = rs.getString(1);
			String activity = rs.getString(2);
			Double completion = (double) rs.getTimestamp(3).getTime();
			if (!caseID.equals(currentID)) {
				if (currentSequence != null) {
					sequences.add(currentSequence);
					times.add(currentTimes);
				}
				currentSequence = new ArrayList<Integer>();
				currentTimes = new ArrayList<Double>();
				currentID = caseID;
				//this is the start event, so:
				previousCompletionTime = previousStartTime;
				previousStartTime = completion;
			}
			currentSequence.add(labelToNumber(activity));
			currentTimes.add(completion - previousCompletionTime);
			previousCompletionTime = completion;
		}
		if ((currentSequence != null) && (!currentSequence.isEmpty())) {
			sequences.add(currentSequence);
			times.add(currentTimes);
		}
				
		stat.close();
		conn.close();
		
		//initialize the chain
		chain = new double[nextId][nextId];
		timing = new double[nextId][nextId];
		
		/*
		 * timesFollowed[i] 
		 * = the number of times start was followed by something, if i == 0
		 * = the number of times symbol i was followed by some other symbol, if i > 0
		 */
		double timesFollowed[] = new double[chain.length];
		double [][] chainCount = new double[chain.length][chain.length]; //the actual chain, but containing follows-counts rather than follows-probabilities
		double [][] totalTime = new double[chain.length][chain.length]; //the total time between completion of [i] and [j]
		
		
		//Compute the number of times i is followed by j, put that in chainCount
		int t = 0;
		for (List<Integer> sequence: sequences){
			List<Double> timeSequence = times.get(t);
			t++;
			if (sequence.size() > 0){
				//times start followed by a symbol
				timesFollowed[0] += 1.0;
				chainCount[0][sequence.get(0)] += 1.0;
				totalTime[0][sequence.get(0)] += timeSequence.get(0);
				//times a symbol followed by end
				timesFollowed[sequence.get(sequence.size()-1)] += 1.0;
				chainCount[sequence.get(sequence.size()-1)][0] += 1.0;
				totalTime[sequence.get(sequence.size()-1)][0] += timeSequence.get(sequence.size()-1);
			}
			for (int i = 0; i < sequence.size() - 1; i++){
				chainCount[sequence.get(i)][sequence.get(i+1)] += 1.0;
				timesFollowed[sequence.get(i)] += 1.0;
				totalTime[sequence.get(i)][sequence.get(i+1)] += timeSequence.get(i+1);
			}
		}
		
		//Rework the number of times i is followed by j, into the probability that i is followed by j, put that in chain
		for (int i = 0; i < chain.length; i++){
			for (int j = 0; j < chain.length; j++){
				timing[i][j] = (totalTime[i][j]/chainCount[i][j])/1000; //in seconds 
				chain[i][j] = chainCount[i][j]/timesFollowed[i];
			}			
		}
	}
	
	/**
	 * Returns the number that uniquely identifies the given activity label.
	 * 
	 * @param label an activity label
	 * @return a number
	 */
	public int labelToNumber(String label) {
		Integer result = activity2Id.get(label);
		if (result == null) {
			activity2Id.put(label, nextId);
			id2Activity.put(nextId, label);
			result = nextId;
			nextId++;
		}
		return result;
	}
	
	/**
	 * Returns the label that is uniquely identified by the given number.
	 * Returns null if no such label exists.
	 * 
	 * @param number a number
	 * @return an activity label or null
	 */
	public String numberToLabel(int number) {
		return id2Activity.get(number);
	}
	
	/*
	 * Generates a random execution sequence based on the current Markov chain.
	 * The execution starts from the given start time (in seconds) and
	 * has the given case identifier.
	 * 
	 * previousStart a start time in seconds
	 * identifier a unique case identifier
	 * Object[2], where
	 * Object[0] = a comma-separated execution sequence of the form caseID, activity, time
	 * Object[1] = the time at which the first event occurred
	 */
	private Object[] generateSequence(double startTime, String identifier) {		
		String sequence = "";
		int previous = 0;
		int next = selectNext(0);
		long nextTime = (long) startTime;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd hh:mm:ss");
		Double firstEventTime = null;
		while (next != 0) {
			nextTime = (long) (nextTime + timing[previous][next]);
			Date time = new Date(nextTime * 1000);
			if (firstEventTime == null) firstEventTime = (double) nextTime;
			
			sequence += identifier + "," + numberToLabel(next) + "," + sdf.format(time) + "\n";
			previous = next;
			next = selectNext(next);
		}
		Object result[] = new Object[2];
		result[0] = sequence;
		result[1] = firstEventTime;
		return result;
	}
	
	public int selectNext(int from) {
		double p = Math.random();
		double sumP = 0.0;
		for (int to = 1; to < chain.length; to++) {
			sumP += chain[from][to];
			if (p <= sumP) {
				return to;
			}
		}
		return 0;
	}
	
	/**
	 * Generates a log of the given size.
	 * The start time of the first case in the log will be 'now'.
	 * The number of cases to generate can be specified.
	 * The name of the file to which the log is saved can also be specified.
	 * The file has a header row: "Case ID,Activity,Completion Time" (without quotes).
	 * 
	 * @param nrCases the number of cases in the log to generate
	 * @param filePath the path of the file to save the log to
	 * @return a comma-separated execution sequence of the form caseID, activity, time
	 * @throws FileNotFoundException 
	 */
	public void generateLog(int nrCases, String filePath) throws FileNotFoundException {
		PrintWriter out = new PrintWriter(filePath);
		out.println("Case ID,Activity,Completion Time");
		double startTime = System.currentTimeMillis()/1000.0;
		for (int i = 0; i < nrCases; i++) {
			Object o[] = generateSequence(startTime,Integer.toString(i));
			startTime = (Double) o[1];
			out.print((String) o[0]);
		}
		out.flush();
		out.close();
	}

	/**
	 * Casts the Markov chain to a string, rounds probabilities to 2 decimals.
	 * Rounds times to 0 decimals.
	 */
	public String toString(){
		String result = " \t";
		for (int j = 1; j < chain.length; j++){
			result += j + "\t";
		}
		result += "stop\n";
		for (int i = 0; i < chain.length; i++){
			result += (i == 0)?"start":i;
			result += "\t";
			for (int j = 1; j < chain.length; j++){
				result += Math.round(chain[i][j]*100.0)/100.0 + "\t";
			}
			result += Math.round(chain[i][0]*100.0)/100.0;
			result += "\n";
		}

		result += " \n\t";
		for (int j = 1; j < chain.length; j++){
			result += j + "\t";
		}
		result += "stop\n";
		for (int i = 0; i < chain.length; i++){
			result += (i == 0)?"start":i;
			result += "\t";
			for (int j = 1; j < chain.length; j++){
				result += Math.round(timing[i][j]) + "\t";
			}
			result += Math.round(timing[i][0]);
			result += "\n";
		}

		return result;
	}
	
}
