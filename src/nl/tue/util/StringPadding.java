package nl.tue.util;

public class StringPadding {

	public static String rpad(int number) {
		return rpad(Integer.toString(number), 15);
	}
	public static String rpad(long number) {
		return rpad(Long.toString(number), 15);
	}
	public static String rpad(String s) {
		return rpad(s, 15);
	}
	
	public static String rpad(int number, int nrColumns) {
		return rpad(Integer.toString(number), nrColumns);
	}
	public static String rpad(long number, int nrColumns) {
		return rpad(Long.toString(number), nrColumns);
	}
	public static String rpad(String s, int nrColumns) {
		String padding = "                         ";
		if (nrColumns > padding.length()) {
			for (int i = 0; i < nrColumns; i++) {
				padding += " ";
			}
		}
	    return (s + padding).substring(0, nrColumns);
	}

}
