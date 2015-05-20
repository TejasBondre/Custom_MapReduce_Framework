import java.io.IOException;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Iterator;


public class WordCount {

	static String foldername = "test";

	public static IMapper mapper;
	public static IReducer reducer;

	public static void execute() throws IOException {

		mapper = new Mapper();
		reducer = new Reducer();

	}

}

class Mapper implements IMapper<Integer, String, String, Integer> {


	private final static Integer one = new Integer(1);
	private String word = new String();

	@Override
	public void map(Integer k, String v, OutputCollector o) {
		String line = v.toString();
		StringTokenizer itr = new StringTokenizer(line);
		while (itr.hasMoreTokens()) {
			word = itr.nextToken();
		    o.collect(word, one);
		}
	}

}

class Reducer implements IReducer<String, String, String, Integer> {

	@Override
	public void reduce(String k, Iterator<String> values, OutputCollector o) {

		Map<String,Integer> wordCount = new LinkedHashMap<>();

		while (values.hasNext()) {

		    String split[] = values.next().split(":");
		    if(wordCount.containsKey(split[0])) {
		    	wordCount.put(split[0], wordCount.get(split[0]) + Integer.parseInt(split[1]));
		    }
		    else {
		    	wordCount.put(split[0], Integer.parseInt(split[1]));
		    }
		}

		o.setState(wordCount);
	}

}

class LexicographicComparator implements Comparator<String> {
    @Override
    public int compare(String a, String b) {
        return a.compareToIgnoreCase(b);
    }
}