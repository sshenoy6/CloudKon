import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.amazonaws.services.sqs.AmazonSQS;


public class ThreadPoolOfWorkers{
	
	public Queue<Task> jobQueue= new LinkedList<Task>();
	public int poolSize;
	public static String sqsQueueURL;
	public static String sqsResponseQueueURL;
	public static AmazonSQS sqs = null;
	
	public void taskRunner(){
		
		ExecutorService executor = null;
		try{
			executor = Executors.newFixedThreadPool(this.poolSize); //initialise the  threadpool
		}catch(Exception e){
			System.out.println("Error in initialising pool");
		}
		
		ListIterator<Task> linkedListIter = (ListIterator<Task>) this.jobQueue.iterator();
		
		
		while(linkedListIter.hasNext()){
			
			Task element = linkedListIter.next();
			if(element!= null){
				if(! element.getTaskDesc().split(" ")[1].matches("\\d+")){
				//if sleep time is not an integer, fail the task
				Client.responses.add("Task "+element.getTaskId()+"  failed");
				continue;
				}
			}
			int sleepTime = 0;
			try{
				sleepTime = Integer.parseInt(element.getTaskDesc().split(" ")[1]);//read the task description and split by space to extract the sleep interval
			}catch(Exception e){
				Client.responses.add("Task "+element.getTaskId()+"  failed");
			}
			Runnable worker = new ThreadWorker(element.getTaskId(),sleepTime); //call to run the thread
			executor.execute(worker);
		}
		executor.shutdown();
		while(!executor.isTerminated()){
			
		}
	}
}