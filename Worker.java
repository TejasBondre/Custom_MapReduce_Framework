import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
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

public class Worker implements Watcher {

    private ZooKeeper zk;
    private String hostPort;
    private String serverId = Integer.toHexString((new Random()).nextInt());
    private volatile boolean connected = false;
    private volatile boolean expired = false;
    private String name;
    private TaskTracker taskTracker;

    public void setTaskTracker(TaskTracker tt) {
    	taskTracker = tt;
    }

    TaskTracker getTaskTracker() { return taskTracker; }

    /**
     * Creates a new Worker instance.
-     *
     * @param hostPort
     */
    public Worker(String hostPort, TaskTracker tt) {
        this.hostPort = hostPort;
        this.taskTracker = tt;
    }

    /**
     * Creates a ZooKeeper session.
     *
     * @throws IOException
     */
    public void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 100000, this);
    }

    /**
     * Deals with session events like connecting
     * and disconnecting.
     *
     * @param e new event generated
     */
    public void process(WatchedEvent e) {
        if(e.getType() == Event.EventType.None){
            switch (e.getState()) {
            case SyncConnected:
                /*
                 * Registered with ZooKeeper
                 */
                connected = true;
                break;
            case Disconnected:
                connected = false;
                break;
            case Expired:
                expired = true;
                connected = false;
                System.out.println("Session expired");
            default:
                break;
            }
        }
    }

    /**
     * Checks if this client is connected.
     *
     * @return boolean
     */
    public boolean isConnected() {
        return connected;
    }

    /**
     * Checks if ZooKeeper session is expired.
     *
     * @return
     */
    public boolean isExpired() {
        return expired;
    }

    /**
     * Bootstrapping here is just creating a /assign parent
     * znode to hold the tasks assigned to this worker.
     */
    public void bootstrap(){
        createAssignNode();
    }


    void createAssignNode(){
        zk.create("/assign/worker-" + serverId, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                createAssignCallback, null);
    }

    StringCallback createAssignCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                /*
                 * Try again. Note that registering again is not a problem.
                 * If the znode has already been created, then we get a
                 * NODEEXISTS event back.
                 */
                createAssignNode();
                break;
            case OK:
            	  break;
            case NODEEXISTS:
            	  break;
            default:
            	System.out.println("Something went wrong: " + KeeperException.create(Code.get(rc), path));
            }
        }
    };







    /**
     * Registering the new worker, which consists of adding a worker
     * znode to /workers.
     */
    public void register(){
        name = "worker-" + serverId;
        zk.create("/workers/" + name,
                "Idle".getBytes(),
                Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                createWorkerCallback, null);
    }

    StringCallback createWorkerCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                /*
                 * Try again. Note that registering again is not a problem.
                 * If the znode has already been created, then we get a
                 * NODEEXISTS event back.
                 */
                register();

                break;
            case OK:
            	                break;
            case NODEEXISTS:

                break;
            default:
            	System.out.println("Something went wrong: ");
            }
        }
    };











    Watcher newTaskWatcher = new Watcher(){
        public void process(WatchedEvent e) {
            if(e.getType() == EventType.NodeChildrenChanged) {
                assert new String("/assign/worker-"+ serverId ).equals( e.getPath() );

                getTasks();
            }
        }
    };


    public void getTasks(){
        zk.getChildren("/assign/worker-" + serverId,
                newTaskWatcher,
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
                    /*new Runnable() {
                        List<String> children;
                        DataCallback cb;

                        public Runnable init (List<String> children, DataCallback cb) {
                            this.children = children;
                            this.cb = cb;

                            return this;
                        }

                        public void run() {
                            if(children == null) {
                                return;
                            }

                            setStatus("Working");
                            for(String task : children){
                                zk.getData("/assign/worker-" + serverId  + "/" + task,
                                        false,
                                        cb,
                                        task);
                            }
                        }
                    }.init(children, taskDataCallback);*/



     //           	setStatus("Working");

                	for(String task : children){
                        zk.getData("/assign/worker-" + serverId  + "/" + task,
                                false,
                                taskDataCallback,
                                task);
                    }

                }
                break;
            default:
                System.out.println("getChildren failed: " + KeeperException.create(Code.get(rc), path));
            }
        }
    };



    DataCallback taskDataCallback = new DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat){
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                zk.getData(path, false, taskDataCallback, null);
                break;
            case OK:
                /*
                 *  Executing a task in this example is simply printing out
                 *  some string representing the task.
                 */



	XThread mapperThread = new XThread(data, ctx, taskTracker, zk);
                mapperThread.start();
                try { mapperThread.join(); } catch (Exception e) { e.printStackTrace(); }



                break;
            default:
            	System.out.println("Failed to get task data: " + KeeperException.create(Code.get(rc), path));
            }
        }
    };


    StringCallback taskStatusCreateCallback = new StringCallback(){
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                zk.create(path + "/status", "done".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                        taskStatusCreateCallback, null);
                break;
            case OK:
                break;
            case NODEEXISTS:
            	System.out.println("Node exists: " + path);
                break;
            default:
            	System.out.println("Failed to create task data: " + KeeperException.create(Code.get(rc), path));
            }

        }
    };

    VoidCallback taskVoidCallback = new VoidCallback(){
        public void processResult(int rc, String path, Object rtx){
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                break;
            case OK:
            	  break;
            default:
            	System.out.println("Failed to delete task data" + KeeperException.create(Code.get(rc), path));
            }
        }
    };

    StatCallback statusUpdateCallback = new StatCallback() {
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                updateStatus((String)ctx);
                return;
			default:
				break;
            }
        }
    };

    String status;
    synchronized private void updateStatus(String status) {
        if (status == this.status) {
            zk.setData("/workers/" + name, status.getBytes(), -1,
                statusUpdateCallback, status);
        }
    }

    public void setStatus(String status) {
        this.status = status;
        updateStatus(status);
    }











    Watcher mapCompletionWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if(e.getType() == EventType.NodeCreated) {
                assert "/mapComplete".equals( e.getPath() );


            	/* Need to write the intermediate data of map out */
            	taskTracker.getMapperOC().write(taskTracker.getDataStore(),taskTracker.getMapperOC().getState());

            	try {
					taskTracker.runReduceTask();
				} catch (KeeperException | InterruptedException e1) {
					System.out.println("Exception while Reduce()");
					e1.printStackTrace();
				}

            }
        }
    };

    public void registerMapComplete(){
        try {
			zk.exists("/mapComplete",mapCompletionWatcher);
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
    }

    StatCallback mapCompleteCallback = new StatCallback() {

		@Override
		public void processResult(int rc, String path, Object ctx, Stat stat) {
			switch(Code.get(rc)) {
            case CONNECTIONLOSS:


                break;
            case OK:

            	/* Need to write the intermediate data of map out */
            	taskTracker.getMapperOC().write(taskTracker.getDataStore(),taskTracker.getMapperOC().getState());

            	try {
					taskTracker.runReduceTask();
				} catch (KeeperException | InterruptedException e) {
					System.out.println("Exception while Reduce()");
					e.printStackTrace();
				}

            	/* start reduce operation */
                break;
            default:
            	try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
            	registerMapComplete();
            	System.out.println("mapComplete node callback failed" +
                        KeeperException.create(Code.get(rc), path));
            }
		}
	};






	public void informReduceComplete() throws KeeperException, InterruptedException {
		zk.create("/mapComplete/worker-" + serverId ,"reduceDone".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);


		expired = true;
	}







    class XThread extends Thread {


    	byte[] data;
        Object ctx;
        TaskTracker tt;
        ZooKeeper zk;

    	public XThread(byte[] data, Object ctx, TaskTracker tt, ZooKeeper zk) {
    		this.data = data;
            this.ctx = ctx;
            this.tt = tt;
            this.zk = zk;
    	}

        public void run() {

            try {
    			tt.runMapTask(new String(data));
    		} catch (IOException e) {
    			e.printStackTrace();
    		}

            zk.create("/status/" + (String) ctx, "done".getBytes(), Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT, taskStatusCreateCallback, null);
        
		try {

		//	 Stat stat;
                  //  if ((stat=zk.exists(,null)) != null)     zk.delete(zPath,stat.getVersion());




				zk.delete("/assign/worker-" + serverId + "/" + (String) ctx,
				        -1);
			} catch (InterruptedException | KeeperException e) {
				System.out.println("Node not deleted");
				e.printStackTrace();
			}

	}
    }
















    public static void main(String args[]) throws Exception {
        /*Worker w = new Worker(args[0]);
        w.startZK();

        while(!w.isConnected()){
            Thread.sleep(100);
        }

         * bootstrap() create some necessary znodes.

        w.bootstrap();


         * Registers this worker so that the leader knows that
         * it is here.

        w.register();


         * Getting assigned tasks.

        w.getTasks();

        while(!w.isExpired()){
            Thread.sleep(1000);
        }
*/






    	ZooKeeper zk = new ZooKeeper("localhost:2181", 15000, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				// TODO Auto-generated method stub

			}});

    	//delete(zk, "/status");
    	//delete(zk, "/assign");
    	//delete(zk, "/workers");
     	//delete(zk, "/tasks");
   	delete(zk, "/mapComplete");




    }

	static void delete(ZooKeeper zk,String zPath) throws InterruptedException, KeeperException {

				try {
		    for (    String child : zk.getChildren(zPath,false))     delete(zk,zPath + "/" + child);
		    Stat stat;
		    if ((stat=zk.exists(zPath,null)) != null)     zk.delete(zPath,stat.getVersion());
		  }
		 catch (  KeeperException e) {
		    throw e;
		  }
	}



}


/*
http://foreignpolicy.com/2015/04/17/they-were-just-struggling-to-breathe-syria-chlorine-gas-attacks/

http://www.politico.com/magazine/story/2015/04/israel-nuclear-weapons-117014.html#.VS7_UJTF_PI
*/


/*

11 cat
12 dog
5 acat
1 dope
1 bass




5 hdog
5 lcat


5 pcat
5 rdog
6 pdog
1 super
1 rude

1 wrong
1 zynga
4 zdog
6 zcat
*/
