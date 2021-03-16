

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ActionThreadPoolExecutor {

	public void getStarted() throws ClassNotFoundException, SQLException {
		ScheduleChunk scd = new ScheduleChunk();
		System.out.println("Before start of all threads");
		ScheduleDAO sdo= new ScheduleDAO();
		if (sdo.getBlockChainSettings().equalsIgnoreCase("N") ) {
			
			ArrayList<HashMap<String, Object>> eventsList = scd.getPendingTrades();
			System.out.println("List of Pending Trades");
			if (eventsList.size() > 0) {

				ExecutorService executor = Executors.newFixedThreadPool(eventsList.size());// creating a pool of 1000
																							// threads
				for (int i = 0; i < eventsList.size(); i++) {
					Runnable worker = new WorkerThread((int) eventsList.get(i).get("eventId"), (String) eventsList.get(i).get("startTime"));
					System.out.println("List of run workers");
					executor.execute(worker);// calling execute method of ExecutorService
				}
				executor.shutdown();
				while (!executor.isTerminated()) {
				}

			}
			System.out.println("Finished all threads");

		} else {
			
		}
			}
}
