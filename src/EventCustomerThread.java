

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

class EventCustomerThread implements Runnable {
	public  int customerId;
	public int eventCustomerMapping;
	public  String startTime;
	public int eventId;
	
	public EventCustomerThread(int customerId,int eventCustomerMapping, String startTime, int eventId) {
		this.customerId = customerId;
		this.startTime = startTime;
		this.eventId = eventId;
		this.eventCustomerMapping = eventCustomerMapping;
	}

	public void run() {
		try {
		ScheduleDAO sdc= new ScheduleDAO();
			ArrayList<HashMap<String,Object>> listOfCustomerData=sdc.getEventCustomerData(customerId,eventCustomerMapping, eventId);
			//Invoke node Api
			System.out.println("Start EventCustomerThread");
			if (listOfCustomerData.size() > 0) {

				ExecutorService executor = Executors.newFixedThreadPool(listOfCustomerData.size());// creating a pool of 1000
																							// threads
				for (int i = 0; i < listOfCustomerData.size(); i++) {
					Runnable worker = new EventCustomerSwitchesThread(
							(String)listOfCustomerData.get(i).get("kiotDeviceId"),
							(int)listOfCustomerData.get(i).get("kiotUserMappingId"),
							(String)listOfCustomerData.get(i).get("kiotUserId"),
							(String)listOfCustomerData.get(i).get("bearerToken"),
							(String)listOfCustomerData.get(i).get("customData"),
							"",
							eventId, customerId,(int)listOfCustomerData.get(i).get("deviceTypeId"),(String)listOfCustomerData.get(i).get("deviceName"));
					System.out.println("List of run EventCustomerThread");
					executor.execute(worker);// calling execute method of ExecutorService
				}
				executor.shutdown();
				while (!executor.isTerminated()) {
				}

		}
			//TimeUnit.SECONDS.sleep(10);
			//getTxData();
		 
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		finally {
			if (ScheduleDAO.con != null) {
//				try {
//			//		ScheduleDAO.con.close();  Close later
//				} catch (SQLException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
			}
				}
		System.out.println(Thread.currentThread().getName() + " (End)");// prints thread name
	}

	private void processmessage() {
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public void getTxData() throws ClassNotFoundException, SQLException, JSONException, IOException {
		ScheduleDAO scd= new ScheduleDAO();
		if (scd.getEventCustomerStatus(customerId, eventId) != 12) {

			JSONArray meterArray = new JSONArray();
			double meterReading=0;
			DBHelper dbhelper = new DBHelper();
			HttpConnectorHelper httpconnectorhelper= new HttpConnectorHelper();
			JSONObject inputDetails1= new JSONObject();
			inputDetails1.put("meterId", 6);
			//inputDetails1.put("timestamp", "2020-06-19 19:15:00");
			inputDetails1.put("timestamp", this.startTime);
			ArrayList<JSONObject> responseFromDevice = httpconnectorhelper
					.sendPostWithToken(scd.getConnectionString(customerId,eventId), inputDetails1);
			// HashMap<String,String> responseAfterParse =
			// cm.parseInput(responseFrombcnetwork);
			if ((boolean)responseFromDevice.get(1).get("error")) {
				dbhelper.updateEventCustomer(meterReading,eventId,customerId,"e");
			}
			if(responseFromDevice.get(0).isNull("meterData")) {
				meterReading = 0;
			} else {
				meterArray = (JSONArray)responseFromDevice.get(0).get("meterData");
				if (meterArray.length() > 0) {
				JSONObject js =(JSONObject) meterArray.get(0);
				meterReading = (double)js.get("meterReading");
				}
				
			}
				dbhelper.updateEventCustomer(meterReading,eventId,customerId,"ne");
			
		}	
	}
	
}