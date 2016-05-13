import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;


/**
 * Class to create a queue of jobs by reading from file
 * @author swathi
 *
 */
public class Client {

	public static Set<String> responses = new HashSet<String>();
	/**
	 * Method to read file and create queue of tasks
	 * @param workload_file
	 * @return
	 */
	public Queue<Task> createListOfTasks(String workload_file){
		Queue<Task> taskList = new LinkedList<Task>();
		FileReader fis = null;
		BufferedReader reader = null;
		String taskDesc;
		int taskId = 0;
		try {
			fis = new FileReader(workload_file);
			reader = new BufferedReader(fis);			
			while((taskDesc = reader.readLine()) != null){ //read line wise and populate in memory queue
				taskId++;
				Task task = new Task();
				task.setTaskId(taskId);
				task.setTaskDesc(taskDesc);
				taskList.add(task);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				fis.close();
				reader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return taskList;
	}
}
