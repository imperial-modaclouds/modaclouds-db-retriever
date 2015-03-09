package imperial.modaclouds.fg.dbretriever;

import java.util.ArrayList;

public class ValueSet {
	
	/**
	 * The values of the monitoring metric
	 */
	private ArrayList<String> values;
	
	/**
	 * The timestamps of the monitoring metric
	 */
	private ArrayList<String> timestamps;
	
	/**
	 * Add new value and timestamp to the variables.
	 * 
	 * @param value	the monitoring value
	 * @param timestamp	the timestamps
	 */
	public void add(String value, String timestamp) {
		values.add(value);
		timestamps.add(timestamp);
	}
	
	/**
	 * The constuctor of the class.
	 * 
	 * @param value	the monitoring value
	 * @param timestamp	the timestamps
	 */
	public ValueSet(String value, String timestamp) { 
		values = new ArrayList<String>();
		timestamps = new ArrayList<String>();
		values.add(value);
		timestamps.add(timestamp);
	}

	/**
	 * Get the values.
	 * @return	the value arrayList.
	 */
	public ArrayList<String> getValues() {
		return values;
	}

	/**
	 * Get the timestamps
	 * @return	the timestamps arrayList.
	 */
	public ArrayList<String> getTimestamps() {
		return timestamps;
	}

	/**
	 * Set the values.
	 * @param values the value arrayList.
	 */
	public void setValues(ArrayList<String> values) {
		this.values = values;
	}

	/**
	 * Set the timestamps
	 * @param timestamps the timestamps arrayList.
	 */
	public void setTimestamps(ArrayList<String> timestamps) {
		this.timestamps = timestamps;
	}
	
	
}
