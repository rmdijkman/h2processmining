# H2 Database Engine for Process Mining

An implementation of the [H2 Database Engine](http://www.h2database.com/) that includes a [weakly follows relation](https://doi.org/10.1109/TKDE.2004.47), which can be used for process mining.

# Use

The package nl.tue.is.weaklyfollows.test contains examples of the use of the weakly follows relation. The package includes performance tests that are based on logs from the BPI Challenges:
- The log from the [BPI 2011 Challenge](https://doi.org/10.4121/uuid:d9769f3d-0ab0-4fb8-803b-0d1120ffcf54)
- The log from the [BPI 2012 Challenge](https://doi.org/10.4121/uuid:3926db30-f712-4394-aebc-75976070e91f)

# Changes

Compared to the vanilla H2 code, this database engine includes the following changes. 

- org.h2.util.WeaklyFollows
	An implementation of the weakly follows relation.
- nl.tue.is.weaklyfollows.test
	The tests for the weakly follows relation, including the performance tests.
- org.h2.expression.Subquery.getValue
	Adapted to return a resultset when the result of the subquery has multiple rows.
- org.h2.command.Parser.readJavaFunction
	Adapted to return the Java function that must be called to create the weakly follows relation, 
	such that this function does not have to be added manually. 
	In future work, the function should be added to the 'org.h2.expression.Function' class. 
	We must then make sure that org.h2.command.Parser.readFunction invokes it. 
	(It does this in the switch, when no Java function by the same name is found.) 
	Through this approach, it is also possible to 'optimize' for the newly added function.
	Note that the function invocation with subquery creates an internal statement, 
	which it does not close, because otherwise the ResultSet would be unavailable. 
	Consequently, we should close it now that we are done. This is not very neat. 
	In future work this should be improved.

# Known Bug
	
```
SELECT * FROM FOLLOWS(SELECT * FROM Event_Log) 
  WHERE (event_label_p = 'A' or event_label_p = 'B' or event_label_p = 'C') and 
        (event_label_s = 'A' or event_label_s = 'B' or event_label_s = 'C')
```
returns:
```
A B
A C
A B
A C
A B
A C
```
The answer should be only A B and A C.
	