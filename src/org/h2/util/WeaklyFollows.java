package org.h2.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.h2.tools.SimpleResultSet;

/**
 * An implementation of the weakly follows relation as a function that can be invoked
 * in an SQL query.
 * 
 */
public class WeaklyFollows {
	
	/**
	 * Implements the weakly follows relation. Given an event log, returns the weakly follows relation for that
	 * event log, in terms of the event labels of events that directly follow each other in some case.
	 * 
	 * The event log must have three columns: 
	 * - case identifier
	 * - event label
	 * - timestamp
	 * The columns must be given in that order, but must not necessarily have a particular name.
	 * The timestamp must be of a 'Time' SQL type.
	 * 
	 * The returned weakly follows relation has two columns:
	 * - the event label for the preceding event (EVENT_LABEL_P)
	 * - the event label for the succeeding event (EVENT_LABEL_S)
	 * 
	 * @param conn		The database connection. Automatically passed by H2 through the SQL query of which the invocation is a part.
	 * @param eventLog	An event log as specified above.
	 * @return			The weakly follows relation for the given event log as specified above.
	 * @throws SQLException
	 */
	public static ResultSet weaklyFollows(Connection conn, ResultSet eventLog) throws SQLException {
		
		class LogEvent implements Comparable<LogEvent> {
			long time;
			Object label;
			
			LogEvent(long time, Object label){
				this.time = time;
				this.label = label;
			}
			
			@Override
			public int compareTo(LogEvent e2) {
				if (time - e2.time < 0){
					return -1;
				}else{
					if (time - e2.time > 0){
						return 1;
					}else{
						return 0;
					}
				}
			}
		}
		
		//If there is no data in the eventLog, return an empty result
		if ((eventLog == null) || (eventLog.getMetaData().getColumnCount() != 3)){
			SimpleResultSet result = new SimpleResultSet();
			result.addColumn("EVENT_LABEL_P", Types.VARCHAR, 255, 0);
			result.addColumn("EVENT_LABEL_S", Types.VARCHAR, 255, 0);
			return result;
		}

		//Map of case identifiers to sequences for that case. Each sequence is a list of log events.
		Map<Object,List<LogEvent>> caseId2Sequence = new HashMap<Object,List<LogEvent>>();
		//Map of antecedent log event labels (in a weakly follows relation) to their consequent log event labels.
		Map<Object, Set<Object>> antecedent2consequents = new HashMap<Object, Set<Object>>();
		
		//For each event in the event log:
		while (eventLog.next()){
			Object caseId = eventLog.getObject(1);
			List<LogEvent> sequence = caseId2Sequence.get(caseId);
			if (sequence == null){
				sequence = new ArrayList<LogEvent>();
				caseId2Sequence.put(caseId, sequence);
			}
			//Add the event to the sequence that belongs to the case identifier of that event. 
			sequence.add(new LogEvent(eventLog.getTimestamp(3).getTime(), eventLog.getObject(2)));
		}
		
		//For each sequence that is constructed in this manner:
		for (List<LogEvent> sequence: caseId2Sequence.values()){
			//Sort the sequence.
			Collections.sort(sequence);
			int sa = 0; //start index of the antecedents
			int ea = 0; //end index of the antecedents, this is the index of the last event that has the same timestamp as sequence[sa]
			long aTime = sequence.get(sa).time;
			while ((ea+1 < sequence.size()) && (aTime == sequence.get(ea+1).time)){
				ea++;
			}
			int sc = ea+1; //start index of the consequents
			int ec = sc; //end index of the consequents, this is the index of the last event that has the same timestamp as sequence[sc]
			//While there are consequents:
			while (ec < sequence.size()){
				long cTime = sequence.get(sc).time;
				while ((ec+1 < sequence.size()) && (cTime == sequence.get(ec+1).time)){
					ec++;
				}
				//Add all antecendent/consequent combinations to antecedent2consequents.
				for (int i = sa; i <= ea; i++){
					Set<Object> consequents = antecedent2consequents.get(sequence.get(i).label);
					if (consequents == null){
						consequents = new HashSet<Object>();
						antecedent2consequents.put(sequence.get(i).label, consequents);
					}
					for (int j = sc; j <= ec; j++){
						consequents.add(sequence.get(j).label);
					}
				}
				//Go the the next batch of antercedents/consequents.
				sa = sc;
				ea = ec;
				sc = ea+1;
				ec = sc;
			}
		}
		
		//Create the result.
		SimpleResultSet result = new SimpleResultSet();
		ResultSetMetaData rsm = eventLog.getMetaData();
		result.addColumn("EVENT_LABEL_P", rsm.getColumnType(2), rsm.getPrecision(2), 0);
		result.addColumn("EVENT_LABEL_S", rsm.getColumnType(2), rsm.getPrecision(2), 0);

		//For each antecedent/consequent combination:
		for (Entry<Object,Set<Object>> acs: antecedent2consequents.entrySet()){
			Object a = acs.getKey();
			for (Object c: acs.getValue()){
				result.addRow(a,c);
			}
		}
	    
	    eventLog.getStatement().close();
	    
		return result;
	}
}
