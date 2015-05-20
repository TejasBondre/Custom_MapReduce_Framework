import java.util.LinkedHashMap;
import java.util.Map;


public class OutputCollector {


	private static OutputCollector ocMapper = new OutputCollector();
	private static OutputCollector ocReducer = new OutputCollector();

	private Map<String,Integer> state;

	private OutputCollector() {
		state = new LinkedHashMap<String,Integer>();
	}

	public static OutputCollector getMapperInstance(){
	      return ocMapper;
	}

	public static OutputCollector getReducerInstance(){
	      return ocReducer;
	}

	void setState(Map<String, Integer> wc) {
		state = wc;
	}

	Map<String,Integer> getState() {
		return state;
	}


	void collect(String k, Integer v) {
		if(!state.containsKey(k)) {
			state.put(k, v);
		}
		else {
			Integer p1 = state.get(k);
			state.put(k, p1 + v);
		}
	}

	void write(DataStore ds, Map<String,Integer> finalState) {
		for(String key: finalState.keySet()) {
			ds.write(key, finalState.get(key).toString());
		}
	}

	String serialize() {
		StringBuilder sb = new StringBuilder();
		for(String k : state.keySet()) {
			sb.append(k.toString() + state.get(k));
		}
		return sb.toString();
	}

	public void finalMerge(DataStore ds, Map<String,Integer> finalReducerState) {
		for(String k: finalReducerState.keySet()) {
			ds.writeToMaster(k, finalReducerState.get(k).toString());
		}
	}


}
