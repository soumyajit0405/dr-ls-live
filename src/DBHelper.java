
import java.beans.Customizer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;



public class DBHelper {

	static Connection con;
	 
	 public void updateEventCustomer(double meterReading, int eventId, int userId, String status) throws SQLException, ClassNotFoundException
		{
		try {	
		 //JDBCConnection connref =new JDBCConnection();
		 if (ScheduleDAO.con == null ) {
				con = JDBCConnection.getOracleConnection();
		 } 
		 
			PreparedStatement pstmt = null;
			double actualPower=0;
			if(ScheduleDAO.con!=null)
			{ 
				if (status.equalsIgnoreCase("ne") && meterReading !=0) {
					String query="update event_customer_mapping set customer_net_meter_reading_e=? ,actual_power=(1-(?-customer_net_meter_reading_s)*4),event_customer_status_id=8 where event_id =? and customer_id=?";
					  pstmt=ScheduleDAO.con.prepareStatement(query);
					  pstmt.setDouble(1,meterReading);
					  pstmt.setDouble(2,meterReading);
					  pstmt.setInt(3,eventId); 
					  pstmt.setInt(4,userId); 
					  pstmt.execute();
					  
					  
					  query="select actual_power from event_customer_mapping where event_id=? and customer_id=?";
					  pstmt=ScheduleDAO.con.prepareStatement(query);
					  pstmt.setInt(1,eventId); 
					  pstmt.setInt(2,userId); 
					  ResultSet rs=pstmt.executeQuery();
					  while(rs.next()) {
						  actualPower=rs.getDouble(1);
					  }
					  query="update all_events set actual_power=actual_power+? where event_id=?";
					  pstmt=ScheduleDAO.con.prepareStatement(query);
					  pstmt.setDouble(1,actualPower);
					  pstmt.setInt(2,eventId); 
					  pstmt.execute();
					//  validate(eventId,userId); 
			
				} else {
						String query="update event_customer_mapping set event_customer_status_id=11 where event_id=? and customer_id=?";
						  pstmt=ScheduleDAO.con.prepareStatement(query);
						  pstmt.setInt(1,eventId); 
						  pstmt.setInt(2,userId); 
						  pstmt.execute();
				}
				
			}
			}
		
	 catch(Exception e) {
		 e.printStackTrace();
	 }

		}
	 
	 public void validate( int eventId, int userId) throws SQLException, ClassNotFoundException
		{
		try {	
		 //JDBCConnection connref =new JDBCConnection();
		 if (ScheduleDAO.con == null ) {
				con = JDBCConnection.getOracleConnection();
		 } 
			PreparedStatement pstmt = null;
			double actualPower=0, committedPower=0, bidPrice=0;
			if(ScheduleDAO.con!=null)
			{
				
				String query="select actual_power,commited_power,bid_price from event_customer_mapping  where event_id =? and customer_id=?";
				  pstmt=ScheduleDAO.con.prepareStatement(query);
				  pstmt.setInt(1,eventId); 
				  pstmt.setInt(2,userId); 
				  ResultSet rs=pstmt.executeQuery();
				  while(rs.next()) {
					  actualPower = rs.getDouble("actual_power");
					  committedPower = rs.getDouble("commited_power");
					  bidPrice = rs.getDouble("bid_price");
				  }
				  double fine = (committedPower-actualPower)*(bidPrice*(120/100));
				  if(committedPower >= actualPower) {
					  double earnings = (actualPower/4)*bidPrice;
					  fine = (committedPower-actualPower)*(bidPrice*(120/100));
					  query="update event_customer_mapping set customer_fine="+fine+", is_fine_applicable='Y',earnings="+earnings+"  where event_id =? and customer_id=?";
					  pstmt=ScheduleDAO.con.prepareStatement(query);
					  pstmt.setInt(1,eventId); 
					  pstmt.setInt(2,userId); 
					  pstmt.execute();
					  
				  } else {
					  double earnings = (committedPower/4)*bidPrice;
					  query="update event_customer_mapping set customer_fine=0, is_fine_applicable='N',earnings="+earnings+"  where event_id =? and customer_id=?";
					  pstmt=ScheduleDAO.con.prepareStatement(query);
					  pstmt.setInt(1,eventId); 
					  pstmt.setInt(2,userId); 
					  pstmt.execute();
				  }
						  
					}
			}
		
	 catch(Exception e) {
		 e.printStackTrace();
	 }

		}
	 
//	 public void updateEventCustomer(double meterReading, int eventId, int userId, String status) throws SQLException, ClassNotFoundException
//		{
//		try {	
//		 //JDBCConnection connref =new JDBCConnection();
//		 if (ScheduleDAO.con == null ) {
//				con = JDBCConnection.getOracleConnection();
//		 } 
//			PreparedStatement pstmt = null;
//			String deviceName="";
//			if(ScheduleDAO.con!=null)
//			{
//				if (status.equalsIgnoreCase("ne") && meterReading !=0) {
//					String query="update event_customer_mapping set customer_net_meter_reading_s=?,event_customer_status_id=13 where event_id=? and customer_id=?";
//					  pstmt=ScheduleDAO.con.prepareStatement(query);
//					  pstmt.setDouble(1,meterReading);
//					  pstmt.setInt(2,eventId); 
//					  pstmt.setInt(3,userId); 
//					  pstmt.execute();
//				} else {
//					String query="update event_customer_mapping set event_customer_status_id=11 where event_id=? and customer_id=?";
//					  pstmt=ScheduleDAO.con.prepareStatement(query);
//					  pstmt.setInt(1,eventId); 
//					  pstmt.setInt(2,userId); 
//					  pstmt.execute();
//				}
//					 
//					 
//					
//			}
//			}
//		
//	 catch(Exception e) {
//		 e.printStackTrace();
//	 }
//		}
		
		public void updateEventCustomer( int eventId, int userId) throws SQLException, ClassNotFoundException
		{
		try {	
		 //JDBCConnection connref =new JDBCConnection();
		 if (ScheduleDAO.con == null ) {
				con = JDBCConnection.getOracleConnection();
		 } 
			PreparedStatement pstmt = null;
			int customerStatus=0;
			if(ScheduleDAO.con!=null)
			{
				String query="select event_customer_status_id from event_customer_mapping where event_id=? and customer_id=?";
				  pstmt=ScheduleDAO.con.prepareStatement(query);
				  pstmt.setInt(1,eventId); 
				  pstmt.setInt(2,userId); 
				  ResultSet rs = pstmt.executeQuery();
				  while(rs.next()) {
					  customerStatus = rs.getInt("event_customer_status_id");
				  }
				  if (customerStatus != 12 ) {
					 query="update event_customer_mapping set event_customer_status_id=12 where event_id=? and customer_id=?";
					  pstmt=ScheduleDAO.con.prepareStatement(query);
					  pstmt.setInt(1,eventId); 
					  pstmt.setInt(2,userId); 
					  pstmt.execute();
				  }	 
					 
					
			}
			}
		
	 catch(Exception e) {
		 e.printStackTrace();
	 }

		}

		public void updateEventCustomerToLive( int eventId, int userId) throws SQLException, ClassNotFoundException
		{
		try {	
		 //JDBCConnection connref =new JDBCConnection();
		 if (ScheduleDAO.con == null ) {
				con = JDBCConnection.getOracleConnection();
		 } 
			PreparedStatement pstmt = null;
			int customerStatus=0;
			if(ScheduleDAO.con!=null)
			{
				String query="update event_customer_mapping set event_customer_status_id=13 where event_id=? and customer_id=?";
					  pstmt=ScheduleDAO.con.prepareStatement(query);
					  pstmt.setInt(1,eventId); 
					  pstmt.setInt(2,userId); 
					  pstmt.execute();
				  }	 
					 
					
			}
			
		
	 catch(Exception e) {
		 e.printStackTrace();
	 }

		
}
}
