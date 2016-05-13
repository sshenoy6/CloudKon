public class ThreadWorker implements Runnable {
	public int taskId;
	public int sleepInterval;
	
	public ThreadWorker(int id,int sleepInterval){
		this.taskId = id;
		this.sleepInterval = sleepInterval;
	}
	@Override
	public void run(){
		try {
				Thread.sleep(this.sleepInterval); // run the actual sleep task
				Client.responses.add("Task "+this.taskId+"  done"); //add response to the queue
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			Client.responses.add("Task "+this.taskId+"  failed");	
		}
		catch(Exception e){
			Client.responses.add("Task "+this.taskId+"  failed");
			
		}
	}
	

}
