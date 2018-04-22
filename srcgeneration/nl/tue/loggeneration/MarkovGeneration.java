package nl.tue.loggeneration;

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
 * A Markov chain for sequences of symbols from a given alphabet.
 * The alphabet must consist of consecutive numbers starting at 1. 
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
	double[][] chain;
	double[][] timing;
	
	/*
	 * Used to transform activity labels to numbers (and back) for efficiency.
	 */
	private int nextId;
	private Map<String,Integer> activity2Id;
	private Map<Integer,String> id2Activity;

	/**
	 * Initializes the Markov chain.
	 */
	public MarkovGeneration(){		
		nextId = 1;
		activity2Id = new HashMap<String,Integer>();
		id2Activity = new HashMap<Integer,String>();
	}

	/**
	 * Initializes the Markov chain given the size of the alphabet.
	 * 
	 * @param size the size of the alphabet.
	 */
	public MarkovGeneration(int size){
		this();
		
		chain = new double[size+1][size+1];
		timing = new double[size+1][size+1];
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
	
	/**
	 * Generates a random execution sequence based on the current Markov chain.
	 * The execution starts from the given start time (in seconds) and
	 * has the given case identifier.
	 * 
	 * @param previousStart a start time in seconds
	 * @param identifier a unique case identifier
	 * @return a comma-separated execution sequence of the form caseID, activity, time
	 */
	public String generateSequence(double startTime, String identifier) {
		String result = "";
		int previous = 0;
		int next = selectNext(0);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd hh:mm:ss"); 
		while (next != 0) {
			long unixTime = (long) ((startTime + timing[previous][next])*1000.0);
			Date time = new Date(unixTime);
			
			result += identifier + "," + numberToLabel(next) + "," + sdf.format(time) + "\n";
			previous = next;
			next = selectNext(next);
		}
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
	 * Casts the Markov chain to a string, rounds probabilities to 2 decimals
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
	
	/**
	 * Computes the probability that the given sequence is generated by the Markov chain. 
	 * 
	 * @param sequence the sequence for which to compute the probability
	 * @return the probability that the given sequence is generated by the Markov chain
	 */
	public double sequenceProbability(int[] sequence){
		double probability = 1.0;
		for (int i = 0; i < sequence.length - 1; i++){
			probability *= chain[sequence[i]][sequence[i+1]];
		}
		if (sequence.length > 0){
			probability *= chain[sequence[sequence.length-1]][0];
		}
		return probability;
	}
	
	/**
	 * Returns the most probable sequence that is generated by the Markov chain, ignoring loops
	 * (i.e. each symbol occurs at most once).
	 * 
	 * @return a sequence
	 */
	public Integer[] mostProbableSequence(){
		boolean[] occurringSymbols = new boolean[chain.length];
		int nrOccurringSymbols = 0;		
		int i = 0; //start at start
		List<Integer> result = new ArrayList<Integer>();
		do{
			int mostLikelyFollower = 0;
			double mostLikelyFollowerProbability = 0.0;
			for (int j = 0; j < chain.length; j++){
				if ((chain[i][j] > mostLikelyFollowerProbability) && (!occurringSymbols[j])){
					mostLikelyFollower = j;
					mostLikelyFollowerProbability = chain[i][j]; 
				}
			}
			i = mostLikelyFollower;
			if (i != 0){
				occurringSymbols[i] = true;
				nrOccurringSymbols += 1;
				result.add(i);
			}
		}while ((i != 0) && (nrOccurringSymbols < chain.length-1));
		return result.toArray(new Integer[0]);
	}
	
	/**
	 * Computes and returns the similarity of sequences two sequences in the Markov chain.
	 * The similarity is computed as the probability that sequence s2 is generated from sequence s1
	 * by (accidentally) inserting, deleting, or replacing activities, or the probability that sequence
	 * s1 is generated from sequence s1 by (accidentally) inserting, deleting, or replacing activities.
	 * 
	 * @param s1 a sequence
	 * @param s2 another sequence
	 * @return the similarity of the sequences
	 */
	public double similarity(int[] s1, int[] s2){
		return similarity(s1,s2,0,0,0,0);
	}
	
	private double similarity(int[] s1, int[] s2, int pos1, int pos2, int state1, int state2){
		if ((pos1 >= s1.length) && (pos2 >= s2.length)){
			return 1.0;
		}else if (pos1 >= s1.length){
			return chain[state2][s2[pos2]] * similarity(s1,s2,pos1,pos2+1,state1,s2[pos2]);
		}else if (pos2 >= s2.length){
			return chain[state1][s1[pos1]] * similarity(s1,s2,pos1+1,pos2,s1[pos1],state2);			
		}else if (s1[pos1] == s2[pos2]){
			return similarity(s1,s2,pos1+1,pos2+1,s1[pos1],s2[pos2]);
		}else{
			return Math.max(chain[state1][s1[pos1]] * similarity(s1,s2,pos1+1,pos2,s1[pos1],state2),
				   chain[state2][s2[pos2]] * similarity(s1,s2,pos1,pos2+1,state1,s2[pos2]));
		}
	}
}
