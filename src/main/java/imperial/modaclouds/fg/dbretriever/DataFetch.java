package imperial.modaclouds.fg.dbretriever;

import it.polimi.modaclouds.monitoring.metrics_observer.JSONMonitoringDataParser;
import it.polimi.modaclouds.monitoring.metrics_observer.MonitoringDatum;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections4.map.MultiKeyMap;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;

public class DataFetch 
{
	private static MultiKeyMap data;
	
	private static ArrayList<String[]> metricPair;
	
	private static ArrayList<String> resources;
	
	public static ArrayList<String> getResources() {
		return resources;
	}

	public static void main(String[] args) {
		String ldbURI = "http://54.155.137.28:3030/ds/query";
		String queryString = "SELECT ?g ?s ?p ?o WHERE { GRAPH ?g { ?s ?p ?o} GRAPH ?g { ?s <http://www.modaclouds.eu/rdfs/1.0/monitoringdata#timestamp> ?t FILTER (?t >= 1412769600000 && ?t <= 1412789600000) } }";
		DataFetching(ldbURI,queryString);
	}
	
	public static synchronized ValueSet parseData(MultiKeyMap dataMap, String resource, String metricName) {
		
		if (dataMap == null)
			return null;
		
		ValueSet result = (ValueSet) dataMap.get(resource,metricName);
		
		return result;
	}
	
	public static synchronized void push (String resource, String metricName, String value, String timestamps) {
		if (data == null) {
			data = new MultiKeyMap();
			data.put(resource, metricName, new ValueSet(value, timestamps));
			String[] pair = new String[2];
			pair[0] = resource;
			pair[1] = metricName;
			metricPair.add(pair);
			resources.add(resource);
		}
		else {
			ValueSet valueSet = (ValueSet) data.get(resource, metricName);
			if (valueSet == null) {
				data.put(resource, metricName, new ValueSet(value, timestamps));
				String[] pair = new String[2];
				pair[0] = resource;
				pair[1] = metricName;
				metricPair.add(pair);
				if (!resources.contains(resource)) {
					resources.add(resource);
				}
			}
			else {
				valueSet.add(value, timestamps);
				data.put(resource, metricName, valueSet);
			}
		}
	}
	
    public static ArrayList<String[]> getMetricPair() {
		return metricPair;
	}
    
	public static MultiKeyMap DataFetching(String ldbURI,String queryString)
    //public static void main(String args[])
    {
    	metricPair = new ArrayList<String[]>();
    	resources= new ArrayList<String>();
    	data = null;
    	//String ldbURI = "http://localhost:3030/ds/query";
    	//String queryString = "SELECT * {?s ?p ?o}";
        QueryExecution qexec = QueryExecutionFactory.sparqlService(ldbURI, queryString);
        try {
            ResultSet results = qexec.execSelect();
            
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            
            ResultSetFormatter.outputAsJSON(baos, results);
            String monitoringData = baos.toString("UTF-8");
            
            jsonToMonitoringDatum(monitoringData);
            
        } catch (Exception e) {
        	e.printStackTrace();
        } finally {
            qexec.close();
        }
        
        return data;
    }
	
	public static void jsonToMonitoringDatum(String json) throws IOException {
		JSONParser parser = new JSONParser();  
		try {  

			Object obj = parser.parse(json);  
			JSONObject jsonObject = (JSONObject) obj;  

			JSONObject results = (JSONObject) jsonObject.get("results");  
			JSONArray bindings = (JSONArray) results.get("bindings");  
			Iterator<JSONObject> iterator = bindings.iterator();

			int count = 0;
			String currentDatum = null;
			String metric = null;
			String metricValue = null;
			String resource = null;
			String type = null;
			String timestamp = null;

			while(iterator.hasNext()) {
				JSONObject binding = iterator.next();
				String s = ((JSONObject) binding.get("s")).get("value").toString();
				String p = ((JSONObject) binding.get("p")).get("value").toString();
				String o = ((JSONObject) binding.get("o")).get("value").toString();

				if (s.substring(s.indexOf('/') + 1).equals(currentDatum)) {
					count = count + 1;
				}
				else {
					currentDatum = s.substring(s.indexOf('/') + 1);
					count = 1;
				}

				type = p.substring(p.lastIndexOf("#")+1);

				switch (type) {
				case "metric":
					metric = o;
					break;
				case "value":
					metricValue = o;
					break;
				case "resourceId":
					resource = o;
					break;
				case "timestamp":
					timestamp = o;
					break;
				}
				
				if (count == 4) {
					//System.out.println(resource+" "+metric+" "+metricValue);
					DataFetch.push(resource, metric, metricValue, timestamp);
				}
			}


		} catch (ParseException e) {  
			e.printStackTrace();  
		}  

	}
	
	
}
