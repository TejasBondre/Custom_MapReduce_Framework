import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.KeeperException;

public class TaskTracker {

	static String hostPort = "localhost:2181";
	static int serverId;
	private OutputCollector ocm = OutputCollector.getMapperInstance();
	private OutputCollector ocr = OutputCollector.getReducerInstance();
	private IMapper mapper;
	private IReducer reducer;
	private DataStore ds;

	int frequentlyWritingDataInMapTask = 30;

	public TaskTracker() {
		ds = new JedisC(serverId);
	}

	public IMapper getMapper() {
		return mapper;
	}

	public IReducer getReducer() {
		return reducer;
	}

	public void setMapper(IMapper mapper) {
		this.mapper = mapper;
	}

	public void setReducer(IReducer reducer) {
		this.reducer = reducer;
	}

	public OutputCollector getMapperOC() {
		return ocm;
	}

	public OutputCollector getReducerOC() {
		return ocr;
	}

	public DataStore getDataStore() {
		return ds;
	}

	public void runMapTask(String fileName) throws FileNotFoundException, IOException {

		Task<Integer,String,String,Integer> mapTask = new Task<Integer,String,String,Integer>(getMapper(),getReducer());

		int lineNumber = 1;
		try(BufferedReader br = new BufferedReader(new FileReader(fileName))) {
		    for(String line; (line = br.readLine()) != null; ) {
		    	mapTask.runMapper(lineNumber, line, getMapperOC());
		    	lineNumber++;
		    }

		}
		System.out.println("Map completed" + frequentlyWritingDataInMapTask);
		frequentlyWritingDataInMapTask --;

		if(frequentlyWritingDataInMapTask == 0) {
			final Map<String, Integer> state = Collections.unmodifiableMap(getMapperOC().getState());

			synchronized (getMapperOC().getState()) {
				getMapperOC().setState(new HashMap<String,Integer>());
			}
			new Thread() {
				public void run() {
					getMapperOC().write(ds,state);
				}
			}.start();

			frequentlyWritingDataInMapTask = 30;
		}

	}

	@SuppressWarnings("unchecked")
	public void runReduceTask() throws KeeperException, InterruptedException {

		/*
		 * get data key <It> from ds
		 * for
		 */
		String key = "";

		List<String> data = ds.read();														// get the reducer data from here or
		Task<String,String,String,Integer> reduceTask = new Task<String,String,String,Integer>(getMapper(),getReducer());
		reduceTask.runReducer(key,data.listIterator(), getReducerOC());
		getReducerOC().finalMerge(ds,getReducerOC().getState());
		w.informReduceComplete();

	}


	class Task<K1, V1,K2,V2> {

		private IMapper<K1,V1,K2,V2> mapper;
		private IReducer<K1,V1,K2,V2> reducer;

		public Task(IMapper im, IReducer ir) {
			mapper = im;
			reducer = ir;
		}

		void runMapper(K1 k, V1 v, OutputCollector o) {
			mapper.map(k, v, o);
		}

		void runReducer(K1 k, Iterator<V1> values, OutputCollector o) {
			reducer.reduce(k, values, o);
		}

	}

	private static Worker w;


	public static void main(String ar[]) throws IOException, InterruptedException {

		serverId = Integer.parseInt(ar[0]);

		TaskTracker taskTracker = new TaskTracker();

		WordCount.execute();
		taskTracker.setMapper(WordCount.mapper);
		taskTracker.setReducer(WordCount.reducer);

		w = new Worker(hostPort,taskTracker);

		w.startZK();

        while(!w.isConnected()){
            Thread.sleep(1000);
        }
        /*
         * bootstrap() create some necessary znodes.
         */
        w.bootstrap();

        /*
         * Registers this worker so that the leader knows that
         * it is here.
         */
        w.register();

        /*
         * Getting assigned tasks.
         */
        w.getTasks();

        w.registerMapComplete();

        while(!w.isExpired()){
            Thread.sleep(10000);
        }

	}
}



/*

./redis-trib.rb create --replicas 0 10.244.35.82:7000 10.244.35.98:7001 10.244.35.111:7002 10.244.35.126:7003 10.244.35.140:7004

*/
