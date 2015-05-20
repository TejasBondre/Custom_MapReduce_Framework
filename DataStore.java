import java.util.List;

public interface DataStore {

	void write(String key, String value);
	List<String> read();
	void writeToMaster(String k, String string);

}
