import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class Master implements Watcher {

    private ZooKeeper zk;
    private String hostPort;
    private volatile boolean connected = false;
    private volatile boolean expired = false;

    protected int reduceWorkers = 5;

    /*private Job job;

    void setJob(Job j) {
    	job = j;
    }*/

    /**
     * Creates a new master instance.
     *
     * @param hostPort
     */
    Master(String hostPort) {
        this.hostPort = hostPort;
    }


    /**
     * Creates a new ZooKeeper session.
     *
     * @throws IOException
     */
    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    /**
     * Closes the ZooKeeper session.
     *
     * @throws IOException
     */
    void stopZK() throws InterruptedException, IOException {
        zk.close();
    }

    /**
     * This method implements the process method of the
     * Watcher interface. We use it to deal with the
     * different states of a session.
     *
     * @param e new session event to be processed
     */
    public void process(WatchedEvent e) {
        if(e.getType() == Event.EventType.None){
            switch (e.getState()) {
            case SyncConnected:
                connected = true;
                break;
            case Disconnected:
                connected = false;
                break;
            case Expired:
                expired = true;
                connected = false;
            default:
                break;
            }
        }
    }


    public void bootstrap(){
        createParent("/workers", new byte[0]);
        createParent("/assign", new byte[0]);
        createParent("/tasks", new byte[0]);
        createParent("/status", new byte[0]);
    }

    void createParent(String path, byte[] data){
        zk.create(path,
                data,
                Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                createParentCallback,
                data);
    }

    StringCallback createParentCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                /*
                 * Try again. Note that registering again is not a problem.
                 * If the znode has already been created, then we get a
                 * NODEEXISTS event back.
                 */
                createParent(path, (byte[]) ctx);

                break;
            case OK:
                System.out.println("Parent created");

                break;
            case NODEEXISTS:
            	System.out.println("Parent already registered: " + path);

                break;
            default:
            	System.out.println("Something went wrong: ");
            }
        }
    };


    /**
     * Check if this client is connected.
     *
     * @return boolean ZooKeeper client is connected
     */
    boolean isConnected() {
        return connected;
    }

    /**
     * Check if the ZooKeeper session has expired.
     *
     * @return boolean ZooKeeper session has expired
     */
    boolean isExpired() {
        return expired;
    }






    Watcher workersChangeWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if(e.getType() == EventType.NodeChildrenChanged) {
                assert "/workers".equals( e.getPath() );

                getWorkers();
            }
        }
    };

    void getWorkers(){
        zk.getChildren("/workers",
                workersChangeWatcher,
                workersGetChildrenCallback,
                null);
    }

    ChildrenCallback workersGetChildrenCallback = new ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children){
            switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                getWorkers();
                break;
            case OK:
                System.out.println("Succesfully got a list of workers: "
                        + children.size()
                        + " workers");
          //    reassignAndSet(children);
                break;
            default:
            	System.out.println("Children failed");
            }
        }
    };


    void reassignAndSet(List<String> children){

        if(children!= null) {
            for(String worker : children){
                getAbsentWorkerTasks(worker);
            }
        }
    }

    void getAbsentWorkerTasks(String worker){
        zk.getChildren("/assign/" + worker, false, workerAssignmentCallback, null);
    }

    ChildrenCallback workerAssignmentCallback = new ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children){
            switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                getAbsentWorkerTasks(path);

                break;
            case OK:
                System.out.println("Succesfully got a list of assignments: "
                        + children.size()
                        + " tasks");

                /*
                 * Reassign the tasks of the absent worker.
                 */

                for(String task: children) {
                    getDataReassign(path + "/" + task, task);
                }
                break;
            default:
                System.out.println("getChildren failed" + KeeperException.create(Code.get(rc), path));
            }
        }
    };












    /*
     ************************************************
     * Recovery of tasks assigned to absent worker. *
     ************************************************
     */

    /**
     * Get reassigned task data.
     *
     * @param path Path of assigned task
     * @param task Task name excluding the path prefix
     */
    void getDataReassign(String path, String task) {
        zk.getData(path,
                false,
                getDataReassignCallback,
                task);
    }

    /**
     * Context for recreate operation.
     *
     */
    class RecreateTaskCtx {
        String path;
        String task;
        byte[] data;

        RecreateTaskCtx(String path, String task, byte[] data) {
            this.path = path;
            this.task = task;
            this.data = data;
        }
    }

    /**
     * Get task data reassign callback.
     */
    DataCallback getDataReassignCallback = new DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat)  {
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                getDataReassign(path, (String) ctx);

                break;
            case OK:
                recreateTask(new RecreateTaskCtx(path, (String) ctx, data));

                break;
            default:
            	System.out.println("Something went wrong when getting data " +
                        KeeperException.create(Code.get(rc)));
            }
        }
    };

    /**
     * Recreate task znode in /tasks
     *
     * @param ctx Recreate text context
     */
    void recreateTask(RecreateTaskCtx ctx) {
        zk.create("/tasks/" + ctx.task,
                ctx.data,
                Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                recreateTaskCallback,
                ctx);
    }

    /**
     * Recreate znode callback
     */
    StringCallback recreateTaskCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                recreateTask((RecreateTaskCtx) ctx);

                break;
            case OK:
                deleteAssignment(((RecreateTaskCtx) ctx).path);

                break;
            case NODEEXISTS:
            	System.out.println("Node exists already, but if it hasn't been deleted, " +
                		"then it will eventually, so we keep trying: " + path);
                recreateTask((RecreateTaskCtx) ctx);

                break;
            default:
            	System.out.println("Something wwnt wrong when recreating task" +
                        KeeperException.create(Code.get(rc)));
            }
        }
    };

    /**
     * Delete assignment of absent worker
     *
     * @param path Path of znode to be deleted
     */
    void deleteAssignment(String path){
        zk.delete(path, -1, taskDeletionCallback, null);
    }

    VoidCallback taskDeletionCallback = new VoidCallback(){
        public void processResult(int rc, String path, Object rtx){
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                deleteAssignment(path);
                break;
            case OK:
            	System.out.println("Task correctly deleted: " + path);
                break;
            default:
            	System.out.println("Failed to delete task data" +
                        KeeperException.create(Code.get(rc), path));
            }
        }
    };


























    /*
     ******************************************************
     ******************************************************
     * Methods for receiving new tasks and assigning them.*
     ******************************************************
     ******************************************************
     */

    Watcher tasksChangeWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if(e.getType() == EventType.NodeChildrenChanged) {
                assert "/tasks".equals( e.getPath() );

                getTasks();
            }
        }
    };

    void getTasks(){
        zk.getChildren("/tasks",
                tasksChangeWatcher,
                tasksGetChildrenCallback,
                null);
    }

    ChildrenCallback tasksGetChildrenCallback = new ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children){
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                getTasks();

                break;
            case OK:

                if(children != null){
                	assignTasks(children);
                }

                break;
            default:
            	System.out.println("getChildren failed." +
                        KeeperException.create(Code.get(rc), path));
            }
        }
    };

    void assignTasks(List<String> tasks) {
	for(int i=0; i<tasks.size();i++){
            if(i == 50) {
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
            }
		System.out.println("Task:" + i);
            getTaskData(tasks.get(i));
        }	    


    }

    void getTaskData(String task) {
        zk.getData("/tasks/" + task,
                false,
                taskDataCallback,
                task);
    }

    private int workerTaskAssignmentIndex = 1;

    DataCallback taskDataCallback = new DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat)  {
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                getTaskData((String) ctx);

                break;
            case OK:
                /*
                 * Choose worker at random.
                 */


                List<String> list = null;
				try {
					list = zk.getChildren("/workers",false);
				} catch (KeeperException | InterruptedException e) {
					System.out.println("Failed to get /worker children");
					e.printStackTrace();
				}
                String designatedWorker = list.get(workerTaskAssignmentIndex%list.size());
                /*
                 * (int)Math.ceil(Math.random()*list.size())
                 */
                workerTaskAssignmentIndex ++;
                /*
                 * Assign task to randomly chosen worker.
                 */
                String assignmentPath = "/assign/" +
                        designatedWorker +
                        "/" +
                        (String) ctx;
                createAssignment(assignmentPath, data);

                break;
            default:
            	System.out.println("Error when trying to get task data." +
                        KeeperException.create(Code.get(rc), path));
            }
        }
    };

    void createAssignment(String path, byte[] data){
        zk.create(path,
                data,
                Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                assignTaskCallback,
                data);
    }

    StringCallback assignTaskCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                createAssignment(path, (byte[]) ctx);

                break;
            case OK:
                deleteTask(name.substring( name.lastIndexOf("/") + 1));

                break;
            case NODEEXISTS:
            	System.out.println("Task already assigned");

                break;
            default:
            	System.out.println("Error when trying to assign task." +
                        KeeperException.create(Code.get(rc), path));
            }
        }
    };




























    /*
     * Executes a sample task and watches for the result
     */

    public void submitTask(String task, TaskObject taskCtx){
        taskCtx.setTask(task);
        zk.create("/tasks/task-",
                task.getBytes(),
                Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT_SEQUENTIAL,
                createTaskCallback,
                taskCtx);
    }

    private StringCallback createTaskCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                /*
                 * Handling connection loss for a sequential node is a bit
                 * delicate. Executing the ZooKeeper create command again
                 * might lead to duplicate tasks. For now, let's assume
                 * that it is ok to create a duplicate task.
                 */
                submitTask(((TaskObject) ctx).getTask(), (TaskObject) ctx);

                break;
            case OK:
                ((TaskObject) ctx).setTaskName(name);

                break;
            default:
                System.out.println("Something went wrong" + KeeperException.create(Code.get(rc), path));
            }
        }
    };


    private int mapJobs;

    public void createTasks(String foldername) {				/* need some throttling here  */
	    File dir = new File(foldername);
	    mapJobs = dir.list().length;

		for(File file: dir.listFiles()) {
			this.submitTask(file.getAbsolutePath(), new TaskObject("mapper" , file.getAbsolutePath()));
		}
    }







    /*
     * Once assigned, we delete the task from /tasks
     */
    void deleteTask(String name){
        zk.delete("/tasks/" + name, -1, taskDeleteCallback, null);
    }

    VoidCallback taskDeleteCallback = new VoidCallback(){
        public void processResult(int rc, String path, Object ctx){
            switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                deleteTask(path);

                break;
            case OK:

                break;
            case NONODE:
            	System.out.println("Task has been deleted already");

                break;
            default:
            	System.out.println("Something went wrong here, " +
                        KeeperException.create(Code.get(rc), path));
            }
        }
    };


















    Watcher tasksCompletionWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if(e.getType() == EventType.NodeChildrenChanged) {
                assert "/status".equals( e.getPath() );

                getTaskStatus();
            }
        }
    };

    void getTaskStatus(){
        zk.getChildren("/status",
        		tasksCompletionWatcher,
        		statusGetChildrenCallback,
                null);
    }

    ChildrenCallback statusGetChildrenCallback = new ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children){
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                getTasks();

                break;
            case OK:

                if(children != null && children.size() == mapJobs){
                	 /* job.mapCompleted(); */
                	try {
						zk.create("/mapComplete",
						        null,
						        Ids.OPEN_ACL_UNSAFE,
						        CreateMode.PERSISTENT);

						jobCompletionStatus();

					} catch (KeeperException | InterruptedException e) {
						e.printStackTrace();
					}
                	System.out.println("Master:Map finished");
                }

                break;
            default:
            	System.out.println("getChildren failed." +
                        KeeperException.create(Code.get(rc), path));
            }
        }
    };










    Watcher jobCompletionWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if(e.getType() == EventType.NodeChildrenChanged) {
                assert "/mapComplete".equals( e.getPath() );

                jobCompletionStatus();
            }
        }
    };


    void jobCompletionStatus(){
        zk.getChildren("/mapComplete",
        		jobCompletionWatcher,
        		jobCompletionChildrenCallback,
                null);
    }

    ChildrenCallback jobCompletionChildrenCallback = new ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children){
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                getTasks();

                break;
            case OK:
                if(children != null && children.size() == reduceWorkers ){
                	 System.out.println("Master:Hadoop job finished");
                	 expired = true;
                }

                break;
            default:
            	System.out.println("getChildren failed." +
                        KeeperException.create(Code.get(rc), path));
            }
        }
    };

    public static void setupMaster(String hostPort, String folderName) throws IOException, InterruptedException {
    	Master m = new Master(hostPort);
        m.startZK();
        while(!m.isConnected()){
            Thread.sleep(100);
        }
        /*
         * bootstrap() creates some necessary znodes.
         */
        m.bootstrap();
        /*
         * now runs for master.
         */

        m.createTasks(folderName);

        m.getWorkers();
	Thread.sleep(2000);
        m.getTasks();

        m.getTaskStatus();

        while(!m.isExpired()){
            Thread.sleep(10000);
        }

        m.stopZK();
    }


    public static void main(String args[]) throws Exception {

    }

}








class TaskObject {
    private String task = "";
    private String taskName = "";
    private boolean done = false;
    private boolean succesful = false;
    private CountDownLatch latch = new CountDownLatch(1);

    public TaskObject(String task, String taskname) {
    	this.task = task;
    	this.taskName = taskname;
	}

    String getTask () {
        return task;
    }

    void setTask (String task) {
        this.task = task;
    }

    void setTaskName(String name){
        this.taskName = name;
    }

    String getTaskName (){
        return taskName;
    }

    void setStatus (boolean status){
        succesful = status;
        done = true;
        latch.countDown();
    }

    void waitUntilDone () {
        try{
            latch.await();
        } catch (InterruptedException e) {
            System.out.println("InterruptedException while waiting for task to get done");
        }
    }

    synchronized boolean isDone(){
        return done;
    }

    synchronized boolean isSuccesful(){
        return succesful;
    }

}
