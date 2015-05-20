import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import redis.clients.jedis.Jedis;

public class Job {

	private String hostPort = "localhost:2181";
	private String folderName = "/home/UFAD/paragoke/2015_DC_Project/demo";
	public void mapCompleted() {}

	boolean isComplete() {return false;}

	void displayResult() {
		Jedis j = new Jedis("10.244.35.82",6379);


		 try {
			 FileOutputStream fos = new FileOutputStream("result.txt");
			 PrintStream ps = new PrintStream(fos);
			 for(String s : j.lrange("output", 0, -1)) {
					ps.println(s);
			 }
			 ps.close();
		 }catch (IOException e) { System.err.println(e); }
	}

	float mapProgress() {
		float progress = 0.0f;
		return progress;
	}

	float reduceProgress() {return 0.0f;}

	void sumbitAndWait() throws IOException, InterruptedException {
		Master.setupMaster(hostPort,folderName);
		displayResult();
	}

	public static void main(String ar[]) throws IOException, InterruptedException {

		Job j = new Job();
		System.out.println("Starting map reduce job...");
		j.sumbitAndWait();
	}


}
