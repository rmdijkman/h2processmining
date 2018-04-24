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
import java.util.Random;

import nl.tue.util.StringPadding;

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
	private List<List<Double>> chain;
	private List<List<Double>> timing;
	
	/*
	 * Used to transform activity labels to numbers (and back) for efficiency.
	 */
	private int nextId;
	private Map<String,Integer> activity2Id;
	private Map<Integer,String> id2Activity;
	
	//For random selection
	Random random;

	//For accessing the database
	Connection conn;

	private MarkovGeneration(){		
		nextId = 1;
		activity2Id = new HashMap<String,Integer>();
		id2Activity = new HashMap<Integer,String>();
		random = new Random(System.currentTimeMillis());
	}
	
	@Override
	public MarkovGeneration clone() {
		MarkovGeneration result = new MarkovGeneration();
		
		result.chain = new ArrayList<List<Double>>();
		for (List<Double> chainRow: chain) {
			List<Double> newRow = new ArrayList<Double>();
			result.chain.add(newRow);
			for (double chainElement: chainRow) {
				newRow.add(chainElement);
			}
		}
		
		result.timing = new ArrayList<List<Double>>();
		for (List<Double> timingRow: timing) {
			List<Double> newRow = new ArrayList<Double>();
			result.timing.add(newRow);
			for (double timingElement: timingRow) {
				newRow.add(timingElement);
			}
		}
		
		result.nextId = nextId;
		
		for (Map.Entry<String, Integer> me: activity2Id.entrySet()) {
			result.activity2Id.put(me.getKey(), me.getValue());
			result.id2Activity.put(me.getValue(), me.getKey());
		}		
		
		return result;
	}
	
	private Long timeOfPreviousStartEvent(String dbFile, String tableName, String currentStartEventTime) throws SQLException {
		Statement stat = conn.createStatement();
		
		ResultSet rs = stat.executeQuery("SELECT MAX(ct) FROM (SELECT MIN(CompleteTimestamp) AS ct FROM " + tableName + " GROUP BY CaseID) WHERE ct < '" + currentStartEventTime + "'");
	
		Long result = null;
		if (rs.first() && (rs.getTimestamp(1) != null)) {
			result = rs.getTimestamp(1).getTime();
		}
		
		rs.close();
		
		stat.close();
		
		return result;
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
		conn = DriverManager.getConnection("jdbc:h2:" + dbFile, "sa", "");
		Statement stat = conn.createStatement();
		
		ResultSet rs = stat.executeQuery("SELECT CaseID,Activity,CompleteTimestamp FROM " + tableName + " ORDER BY CaseID,CompleteTimestamp");
		String currentID = "";
		List<List<Integer>> sequences = new LinkedList<List<Integer>>();
		List<List<Double>> times = new LinkedList<List<Double>>();
		List<Integer> currentSequence = null;
		List<Double> currentTimes = new LinkedList<Double>();
		Double previousCompletionTime = null;
		while (rs.next()) {
			String caseID = rs.getString(1);
			String activity = rs.getString(2);
			Double completion = (double) rs.getTimestamp(3).getTime(); //in milliseconds
			if (!caseID.equals(currentID)) {
				if (currentSequence != null) {
					sequences.add(currentSequence);
					times.add(currentTimes);
				}
				currentSequence = new ArrayList<Integer>();
				currentTimes = new ArrayList<Double>();
				currentID = caseID;
				//this is a start event, so the previous completion time is the previous start event
				Long t = timeOfPreviousStartEvent(dbFile, tableName, rs.getString(3));
				if (t == null) {
					previousCompletionTime = completion;
				} else {
					previousCompletionTime = (double) t; 
				}
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
		chain = new ArrayList<List<Double>>();
		timing = new ArrayList<List<Double>>();
		for (int i = 0; i < nextId; i++) {
			chain.add(new ArrayList<Double>(nextId));
			timing.add(new ArrayList<Double>(nextId));			
			for (int j = 0; j < nextId; j++) {
				chain.get(i).add(0.0);
				timing.get(i).add(0.0);
			}
		}
		
		/*
		 * timesFollowed[i] 
		 * = the number of times start was followed by something, if i == 0
		 * = the number of times symbol i was followed by some other symbol, if i > 0
		 */
		double timesFollowed[] = new double[chain.size()];
		double [][] chainCount = new double[chain.size()][chain.size()]; //the actual chain, but containing follows-counts rather than follows-probabilities
		double [][] totalTime = new double[chain.size()][chain.size()]; //the total time between completion of [i] and [j]
		
		
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
		for (int i = 0; i < chain.size(); i++){
			for (int j = 0; j < chain.size(); j++){
				timing.get(i).set(j, (totalTime[i][j]/chainCount[i][j])/1000); //in seconds 
				chain.get(i).set(j, chainCount[i][j]/timesFollowed[i]);
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
	private Object[] generateSequence(double startTime, String identifier) throws LogGenerationException{		
		String sequence = "";
		int size = 0;
		int previous = 0;
		int next = selectNext(0);
		long nextTime = (long) startTime;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd hh:mm:ss");
		Double firstEventTime = null;
		while (next != 0) {
			nextTime = (long) (nextTime + timing.get(previous).get(next));
			Date time = new Date(nextTime * 1000);
			if (firstEventTime == null) firstEventTime = (double) nextTime;
			
			sequence += identifier + ";" + numberToLabel(next) + ";" + sdf.format(time) + "\n";
			size++;
			if (size > chain.size()*100) throw new LogGenerationException("The size of the generated log is unexpectedly large. There is likely an infinite cycle in the process.");
			previous = next;
			next = selectNext(next);
		}
		Object result[] = new Object[2];
		result[0] = sequence;
		result[1] = firstEventTime;
		return result;
	}
		
	public int selectNext(int from) {
		double p = random.nextDouble();
		double sumP = 0.0;
		for (int to = 1; to < chain.size(); to++) {
			sumP += chain.get(from).get(to);
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
	 */
	public void generateLog(int nrCases, String filePath) throws LogGenerationException, FileNotFoundException {
		PrintWriter out = new PrintWriter(filePath);
		out.println("CaseID;Activity;CompleteTimestamp");
		double startTime = System.currentTimeMillis()/1000.0;
		for (int i = 0; i < nrCases; i++) {
			Object o[] = generateSequence(startTime,Integer.toString(i));
			if (((String) o[0]).length() > 0) { //Skip empty sequences
				startTime = (Double) o[1];
				out.print((String) o[0]);
			}
		}
		out.flush();
		out.close();
	}

	/**
	 * Returns true if a log can be generated for this generator.
	 * False if not, in which case there is likely an infinite loop in the generator.
	 * 
	 * @return true or false
	 */
	public boolean testGenerateLog() {
		double startTime = System.currentTimeMillis()/1000.0;
		for (int i = 0; i < 100; i++) {
			Object o[];
			try {
				o = generateSequence(startTime,Integer.toString(i));
			} catch (LogGenerationException e) {
				return false;
			}
			startTime = (Double) o[1];
		}
		return true;
	}
		
	/**
	 * Casts the Markov chain to a string, rounds probabilities to 2 decimals.
	 * Rounds times to 0 decimals.
	 */
	public String toString(){
		String result = " \t";
		for (int j = 1; j < chain.size(); j++){
			result += j + "\t";
		}
		result += "stop\n";
		for (int i = 0; i < chain.size(); i++){
			result += (i == 0)?"start":i;
			result += "\t";
			for (int j = 1; j < chain.size(); j++){
				result += Math.round(chain.get(i).get(j)*100.0)/100.0 + "\t";
			}
			result += Math.round(chain.get(i).get(0)*100.0)/100.0;
			result += "\n";
		}

		result += " \n";
		result += StringPadding.rpad("", 10);
		for (int j = 1; j < chain.size(); j++){
			result += StringPadding.rpad(j, 10);
		}
		result += "stop\n";
		for (int i = 0; i < chain.size(); i++){
			result += (i == 0)?StringPadding.rpad("start", 10):StringPadding.rpad(i, 10);
			for (int j = 1; j < chain.size(); j++){
				result += StringPadding.rpad(Math.round(timing.get(i).get(j)), 10);
			}
			result += Math.round(timing.get(i).get(0));
			result += "\n";
		}

		return result;
	}
	
	/**
	 * Adds the activity with the given label to the generator.
	 * The activity is added in such a way that its occurrence in the log after the occurrence of another activity, 
	 * is as likely as one of the activities that is already in the log.
	 * After the activity occurs, the next activity to occur is determined by a random copy of some existing activity.   
	 * 
	 * @param label the label of the activity that must be added
	 * @throws LogGenerationException 
	 */
	public void addActivity(String label) throws LogGenerationException {
		//select the flow with the highest probability
		int sourceId = -1;
		int targetId = -1;
		double p = 0.0;
		for (int i = 0; i < chain.size(); i++) {
			for (int j = 0; j < chain.size(); j++) {
				if (chain.get(i).get(j) > p) {
					p = chain.get(i).get(j);
					sourceId = i;
					targetId = j;
				}
			}
		}
		if (sourceId == -1) throw new LogGenerationException("There are no sufficiently large generation moves to add an activity.");
		
		//create a the new activity with the given label
		//set chain and timing properties from/to the new activity to 0.0 
		int newActivityId = labelToNumber(label);		
		chain.add(new ArrayList<Double>());
		timing.add(new ArrayList<Double>());
		for (int i = 0; i < chain.size()-1; i++) {
			chain.get(i).add(0.0);
			timing.get(i).add(0.0);			
		}
		for (int i = 0; i < chain.size(); i++) {
			chain.get(chain.size()-1).add(0.0);
			timing.get(chain.size()-1).add(0.0);			
		}
		
		//the outgoing flows from newActivity should be the same as outgoing flows from source
		for (int i = 0; i < chain.size()-1; i++) {
			chain.get(newActivityId).set(i, chain.get(sourceId).get(i));
			timing.get(newActivityId).set(i, timing.get(sourceId).get(i));
		}
		
		//the incoming flows to newActivity should be the same as the incoming flows to the target
		//the probability of these flows should be divided by 2, because there are now 2 possibilities
		for (int i = 0; i < chain.size()-1; i++) {
			double toProbability = chain.get(i).get(targetId) / 2.0d;
			double toTiming = timing.get(i).get(targetId);
			
			chain.get(i).set(targetId, toProbability);
			chain.get(i).set(newActivityId, toProbability);
			timing.get(i).set(newActivityId, toTiming);
		}		
	}
	
	public void removeActivity() {
		//select the activity with the least expected executions
		double expectedExecutions[] = expectedExecutions();
		int lowestExpectationId = -1;
		double lowestExpectation = Double.MAX_VALUE;
		for (int i = 0; i < expectedExecutions.length; i ++) {
			if (expectedExecutions[i] < lowestExpectation) {
				lowestExpectation = expectedExecutions[i];
				lowestExpectationId = i + 1;
			}
		}

		//reset the probabilities for the remaining activities
		for (int i = 0; i < chain.size(); i++) {
			for (int j = 0; j < chain.size(); j++) {
				double newP = chain.get(i).get(j) + chain.get(i).get(lowestExpectationId) * chain.get(lowestExpectationId).get(j);
				chain.get(i).set(j, newP);
			}
		}

		//reset the labels for the remaining activities
		for (int i = lowestExpectationId; i < chain.size()-1; i++) {
			String newLabel = id2Activity.get(i+1);
			activity2Id.put(newLabel, i);
			id2Activity.put(i, newLabel);
		}
		String oldLabel = id2Activity.get(chain.size()-1);
		activity2Id.remove(oldLabel);
		id2Activity.remove(chain.size()-1);
		nextId --;
		
		//remove the activity with the least expected executions
		for (int i = 0; i < chain.size(); i++) {
			chain.get(i).remove(lowestExpectationId);
		}
		chain.remove(lowestExpectationId);				
	}
		
	/**
	 * Returns the probabilities of the initial states of the process.
	 * 
	 * @return an array a, such that a[i] represents the probability that numberToLabel(i) is the initial state 
	 */
	public double[] initialState() {
		double result[] = new double[chain.size()-1];
		for (int i = 1; i < chain.size(); i++) {
			result[i-1] = chain.get(0).get(i);
		}		
		return result;
	}
	
	/**
	 * Returns, given the probabilities of current states, the probabilities of the next states of the process. 
	 * 
	 * @param current an array, such that current[i] represents the probability that numberToLabel(i) is the current state
	 * @return an array a, such that a[i] represents the probability that numberToLabel(i) is the next state
	 */
	public double[] probabilityNextState(double current[]) {
		double result[] = new double[chain.size()-1];
		
		for (int i = 1; i < chain.size(); i++) {
			double inProduct = 0.0;
			for (int j = 1; j < chain.size(); j++) {
				inProduct += current[j-1]*chain.get(j).get(i);
			}
			result[i-1] = inProduct;
		}
		
		return result;
	}
	
	/**
	 * Returns a string representation of the given state probabilities.
	 * 
	 * @param stateProbability an array a, such that a[i] represents the probability that numberToLabel(i) is the state
	 * @return a string representation of the state probability
	 */
	public static String stateProbabilityToString(double stateProbability[]) {
		String result = "[";
		for (int i = 0; i < stateProbability.length; i++) {
			result += Double.toString(Math.round(stateProbability[i]*100.0)/100.0);
			if (i < stateProbability.length - 1) result += ", ";
		}
		result += "]";
		return result;
	}
	
	/**
	 * returns true if the given state probabilities are the same within a margin of 0.01
	 * 
	 * @param stateProbability1 an array representing state probabilities
	 * @param stateProbability2 an array representing state probabilities
	 * @return true or false
	 */
	public static boolean similarStateProbability(double stateProbability1[], double stateProbability2[]) {
		double precision = 0.01;
		for (int i = 0; i < stateProbability1.length; i++) {
			if (Math.abs(stateProbability1[i] - stateProbability2[i]) > precision) return false;
		}
		return true;
	}
	
	/**
	 * Returns the number of times each activity is expected to be executed per case, in an infinite number of case executions
	 * 
	 * @return an array a, such that a[i] represents the number of times numberToLabel(i) is expected to be executed
	 */
	public double[] expectedExecutions() {
		double nrExecutions[] = new double[chain.size()-1];
		
		double state[] = initialState();
		double newState[] = probabilityNextState(state);
		while (!similarStateProbability(state, newState)) {
			for (int i = 0; i < nrExecutions.length; i++) {
				nrExecutions[i] += state[i];
			}
			state = newState;
			newState = probabilityNextState(state);
		}
		
		return nrExecutions;
	}

	/**
	 * Returns the expected total number of activity per case, in an infinite number of case executions
	 * 
	 * @return the expected number of activities per case
	 */
	public double totalExpectedExecutions() {
		double e[] = expectedExecutions();
		double result = 0.0;
		for (int i = 0; i < e.length; i++) {
			result += e[i];
		}
		return result;
	}
	
	/**
	 * Extends the generator in such a way that the expected number of activities per case is at least a factor f of the original.
	 * The method will create the closest generator it can that is at least a factor f. 
	 * The actual factor is likely to be higher f and is returned.
	 * f should be > 1, because the goal is to extend. 
	 * 
	 * @param f the factor that the expected number of executions per case should be increased with
	 * @return the actual factor that the expected number of execution per case has become
	 */
	public double extendByExpectedExecutions(double f) throws LogGenerationException {
		double e = totalExpectedExecutions();
		double eNew = e;
		int additions = 0;
		while (eNew/e < f) {
			addActivity("A" + nextId);
			eNew = totalExpectedExecutions();
			additions ++;
			if (additions > 1000) {
				throw new LogGenerationException("The desired extension could not be realized after 1000 added event types. Breaking off.");
			}
		}
		return eNew/e;
	}
	
	/**
	 * Extends the generator in such a way that the expected number of activities per case is at least a factor f of the original.
	 * The method will create the closest generator it can that is at most a factor f.
	 * The actual factor is likely to be lower f and is returned.
	 * f should be < 1, because the goal is to reduce.
	 *  
	 * @param f the factor by which the expected number of execution per case should be reduced
	 * @return the actual factor that the expected number of execution per case has become
	 */
	public double reduceByExpectedExecutions(double f) {
		double e = totalExpectedExecutions();
		double eNew = e;
		while (eNew/e > f) {
			removeActivity();
			eNew = totalExpectedExecutions();
		}
		return eNew/e;
	}
}
