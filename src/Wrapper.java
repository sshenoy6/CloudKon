import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.util.Tables;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;


/**
 * The interface to behave as client or worker based on Command line arguments
 * @author swathi
 *
 */
public class Wrapper {
	
	static int requestQueueLength = 0;
	static int rQueueLength = 0;
	public static void clientRun(String[] args){
		Client client = new Client();
		if(args[2].equals(null) || !args[1].equalsIgnoreCase("-s")){
			System.out.println("Queue name not given!! Terminating");
			System.exit(0);
		}
		if(args[2].equalsIgnoreCase("LOCAL")){
			Queue<Task> jobQueue = client.createListOfTasks(args[6]);
			ThreadPoolOfWorkers pool = new ThreadPoolOfWorkers();
			if(args[3].equals("-t")){
				int n = Integer.parseInt(args[4]); //number of threads
				pool.jobQueue = jobQueue;
				
				Queue<Task> queueCopy = new LinkedList<Task>();
				queueCopy.addAll(pool.jobQueue);
				
				pool.poolSize = n; //set the threadpool size
				long start = System.nanoTime();
				//invoke the thread pool code to actually run the job
				pool.taskRunner();
				System.out.println("The total time taken is:"+((System.nanoTime() - start)/1000000)+" ms");
			}else{
				pool.jobQueue = jobQueue;
				while(Client.responses.size()== 0){ //keep waiting till responses are populated
					
				}
				FileWriter fw;
				try {
					fw = new FileWriter("responses_local.txt",true);
					BufferedWriter bw = new BufferedWriter(fw);
					Iterator<String> responses_iter =  Client.responses.iterator();
					while(responses_iter.hasNext()){
						bw.write(responses_iter.next());
						bw.newLine();
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
		}else{
			AWSCredentials credentials = null;
			try {
				credentials = new ProfileCredentialsProvider("default").getCredentials();
			} catch (Exception e) {
				throw new AmazonClientException(
						"Cannot load the credentials from the credential profiles file. " +
								"Please make sure that your credentials file is at the correct " +
								"location (/home/swathi/.aws/credentials), and is in valid format.",
								e);
			}
			
			AmazonSQS sqs = new AmazonSQSClient(credentials);
			Region usEast1 = Region.getRegion(Regions.US_EAST_1);
			sqs.setRegion(usEast1);
			ThreadPoolOfWorkers pool = new ThreadPoolOfWorkers();
			long start = 0;
			try {
				// Create a queue for jobs
				CreateQueueRequest createQueueRequest = new CreateQueueRequest(args[2]);
				ThreadPoolOfWorkers.sqsQueueURL = sqs.createQueue(createQueueRequest).getQueueUrl();
				
				//Create a queue for response
				CreateQueueRequest createQueueResponse = new CreateQueueRequest(args[2]+"response");
				ThreadPoolOfWorkers.sqsResponseQueueURL = sqs.createQueue(createQueueResponse).getQueueUrl();
				
				pool.jobQueue = client.createListOfTasks(args[4]); //read from file and populate the list of jobs
				
				//start the timer
				start = System.currentTimeMillis();
				for(Task task: pool.jobQueue){
					sqs.sendMessage(new SendMessageRequest(ThreadPoolOfWorkers.sqsQueueURL,task.getTaskId()+"  "+task.getTaskDesc()));
				}
				//indication that all tasks are on the source queue
				System.out.println("Client --- messages put on SQS queue");
				
				GetQueueAttributesRequest request = new GetQueueAttributesRequest(ThreadPoolOfWorkers.sqsQueueURL).withAttributeNames("All");
			    Map<String,String> queueAttr = sqs.getQueueAttributes(request).getAttributes();
			    rQueueLength = Integer.parseInt(queueAttr.get("ApproximateNumberOfMessages"));
			    
			    
			    //System.out.println("Before the wait loop");
			    //looping to find the net time taken to run the job by all workers
				while(rQueueLength != 0){ //keep waiting till responses are populated
					//Thread.sleep(1000);
					request = new GetQueueAttributesRequest(ThreadPoolOfWorkers.sqsQueueURL).withAttributeNames("All");
				    queueAttr = sqs.getQueueAttributes(request).getAttributes();
				    rQueueLength = Integer.parseInt(queueAttr.get("ApproximateNumberOfMessages"));
				}
				//System.out.println("Out of the wait loop");
				long duration = System.currentTimeMillis() - start;
				
				System.out.println("The total time taken is "+duration);
			}catch(AmazonServiceException e){

			}
		}

	}

	public static void workerRun(String[] args){
		ThreadPoolOfWorkers pool = new ThreadPoolOfWorkers();
			//System.out.println("in worker");
			AWSCredentials credentials = null;
			AmazonDynamoDBClient dynamoDB;
			BufferedWriter bw = null;
			List<Message> response_messages = new ArrayList<Message>();
			pool.poolSize = Integer.parseInt(args[4]); //number of threads
				try{
	            //receive the response
				credentials = new ProfileCredentialsProvider("default").getCredentials();
				ThreadPoolOfWorkers.sqs = new AmazonSQSClient(credentials);
			    Region usEast1 = Region.getRegion(Regions.US_EAST_1);
			    ThreadPoolOfWorkers.sqs.setRegion(usEast1);
			    
			    dynamoDB = new AmazonDynamoDBClient(credentials);
		        dynamoDB.setRegion(usEast1);
			    
			   
			  //extract the message from the SQS request queue and populate the jobQueue
			    ThreadPoolOfWorkers.sqsQueueURL = ThreadPoolOfWorkers.sqs.getQueueUrl(args[2]).getQueueUrl();
			    ThreadPoolOfWorkers.sqsResponseQueueURL = ThreadPoolOfWorkers.sqsQueueURL+"response";
			    
			    //calculate the queue length for the looping condition
			    GetQueueAttributesRequest request = new GetQueueAttributesRequest(ThreadPoolOfWorkers.sqsQueueURL).withAttributeNames("All");
			    Map<String,String> queueAttrResp = ThreadPoolOfWorkers.sqs.getQueueAttributes(request).getAttributes();
			    int reqQueueLength = Integer.parseInt(queueAttrResp.get("ApproximateNumberOfMessages"));
			    
			    while(reqQueueLength != 0){ // loop the source queue for jobs until it becomes empty
			    	ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(ThreadPoolOfWorkers.sqsQueueURL);
			    	List<Message> messages = ThreadPoolOfWorkers.sqs.receiveMessage(receiveMessageRequest).getMessages();
			    	String taskId = "";
			    	try{
			    		for (Message message : messages) { //iterate over the message received from the source queue
			    			Task task = new Task();
			    			taskId = message.getBody().split("  ")[0];
			    			task.setTaskId(Integer.parseInt(taskId)); //split the string and extract the task id and set it to Task object
			    			boolean valid = dynamoDB_validate(dynamoDB,taskId); //check if the task is valid or a duplicate
			    			task.setTaskDesc(message.getBody().split("  ")[1]);//extract the sleep task 
			    			if(valid){
			    				pool.jobQueue.add(task); //add to source job queue to keep track of length
			    				pool.taskRunner(); //call the thread code to actually run the job
			    				ThreadPoolOfWorkers.sqs.sendMessage(new SendMessageRequest(ThreadPoolOfWorkers.sqsResponseQueueURL,"Task "+task.getTaskId()+" done"));//insert the response to response Queue
			    				
			    			//delete message after reading it and putting on jobQueue
			    			if(messages.size() > 0){
			    				//once processing is done and response is added to response queue, delete the message from source queue
			    				String messageReceiptHandle = messages.get(0).getReceiptHandle();
			    				ThreadPoolOfWorkers.sqs.deleteMessage(new DeleteMessageRequest(ThreadPoolOfWorkers.sqsQueueURL, messageReceiptHandle));
			    				
			    				//poll the current length of source queue after deletion
			    				request = new GetQueueAttributesRequest(ThreadPoolOfWorkers.sqsQueueURL).withAttributeNames("All");
			    				queueAttrResp = ThreadPoolOfWorkers.sqs.getQueueAttributes(request).getAttributes();
			    				reqQueueLength = Integer.parseInt(queueAttrResp.get("ApproximateNumberOfMessages"));
			    				}
			    			}else{
			    				
			    				ThreadPoolOfWorkers.sqs.sendMessage(new SendMessageRequest(ThreadPoolOfWorkers.sqsResponseQueueURL,"Task "+task.getTaskId()+" failed"));
			    			}
			    			//poll the current length of source queue after deletion
			    			request = new GetQueueAttributesRequest(ThreadPoolOfWorkers.sqsQueueURL).withAttributeNames("All");
		    				queueAttrResp = ThreadPoolOfWorkers.sqs.getQueueAttributes(request).getAttributes();
		    				reqQueueLength = Integer.parseInt(queueAttrResp.get("ApproximateNumberOfMessages"));
			    			
			    		}
			    		//poll the current length of source queue
			    		request = new GetQueueAttributesRequest(ThreadPoolOfWorkers.sqsQueueURL).withAttributeNames("All");
	    				queueAttrResp = ThreadPoolOfWorkers.sqs.getQueueAttributes(request).getAttributes();
	    				reqQueueLength = Integer.parseInt(queueAttrResp.get("ApproximateNumberOfMessages"));
			    	
			    	
			    	
			    	}catch(OutOfMemoryError e){
	    				
			    		Client.responses.add("Task "+taskId+"  fails");
	    			}
			    	//receive all the response messages one at a time and add to a common list
			    	ReceiveMessageRequest receiveMessageResponse = new ReceiveMessageRequest(ThreadPoolOfWorkers.sqsQueueURL+"response");
			    	response_messages.addAll(ThreadPoolOfWorkers.sqs.receiveMessage(receiveMessageResponse).getMessages());
			    	
			    }
			    //write the responses to a file
			    FileWriter fw = new FileWriter("responses.txt",true);
			    bw = new BufferedWriter(fw);
				Iterator<Message> responses_iter =  response_messages.iterator();
				while(responses_iter.hasNext()){
					bw.write(responses_iter.next().getBody().toString());
					bw.newLine();
				}
			   
				}catch(AmazonServiceException e){

				} catch (InterruptedException e) {
					
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	}
	public static void main(String[] args) {
		if(args[0].equalsIgnoreCase("client")){ //check the command line argument and decide where to branch out to
			clientRun(args);
		}else if(args[0].equalsIgnoreCase("worker")){
			workerRun(args);
			
		}
	}
	/**
	 * Method to validate and insert the taskId into table in DynamoDB
	 * @param dynamoDB
	 * @param taskId
	 * @return
	 * @throws InterruptedException
	 */
	public static boolean dynamoDB_validate(AmazonDynamoDBClient dynamoDB,String taskId) throws InterruptedException{
		String tableName = "TaskTracker2";
		try{
        // Create table if it does not exist yet
        if (Tables.doesTableExist(dynamoDB, tableName)) {
            //System.out.println("Table " + tableName + " is already ACTIVE");
        } else {
            // Create a table with a primary hash key named 'name', which holds a string
            CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
                .withKeySchema(new KeySchemaElement().withAttributeName("taskId").withKeyType(KeyType.HASH))
                .withAttributeDefinitions(new AttributeDefinition().withAttributeName("taskId").withAttributeType(ScalarAttributeType.S))
                .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(20L).withWriteCapacityUnits(20L));
                TableDescription createdTableDescription = dynamoDB.createTable(createTableRequest).getTableDescription();
            //System.out.println("Created Table: " + createdTableDescription);

            // Wait for it to become active
          //  System.out.println("Waiting for " + tableName + " to become ACTIVE...");
            Tables.awaitTableToBecomeActive(dynamoDB, tableName);
        }

        // Describe our new table
        DescribeTableRequest describeTableRequest = new DescribeTableRequest().withTableName(tableName);
        TableDescription tableDescription = dynamoDB.describeTable(describeTableRequest).getTable();
        //System.out.println("Table Description: " + tableDescription);

        // Add an item
        Map<String, AttributeValue> item = newItem(taskId);
        PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
        PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);
        dynamoDB.putItem(putItemRequest);
       // System.out.println("Result: " + putItemResult);
        return true; // means it is not a duplicate - safe to execute the task
		}catch (AmazonServiceException ase) {
            return false; // duplicate task is picked up
        }
        
        
	}
	
	/**
	 * Method to insert data into DynamoDB- taskId of the task that is picked
	 * @param taskId
	 * @return
	 */
	private static Map<String, AttributeValue> newItem(String taskId) {
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("taskId", new AttributeValue(taskId));
        return item;
    }
}
