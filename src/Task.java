
/**
 * Task object containing task id and the actual job description in the form of string
 * @author swathi
 *
 */
public class Task {
	
	private int taskId;
	private String taskDesc;

	/**
	 * @param args
	 */
	public Task(){
		
	}
	public int getTaskId() {
		return taskId;
	}

	public void setTaskId(int taskId) {
		this.taskId = taskId;
	}

	public String getTaskDesc() {
		return taskDesc;
	}

	public void setTaskDesc(String taskDesc) {
		this.taskDesc = taskDesc;
	}
	
}
