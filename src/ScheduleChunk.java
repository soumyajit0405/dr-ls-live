

import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

public class ScheduleChunk {
	



	  public ArrayList<HashMap<String,Object>> getPendingTrades() throws ClassNotFoundException, SQLException {
	       
		  ScheduleDAO scheduledao=new ScheduleDAO();
		  int hours = scheduledao.getConfigValue("load_shift_hours");
		  final long loadShiftHours = hours*3600*1000; 
		  final long ONE_MINUTE_IN_MILLIS=60000;//millisecs
		  final long HOUR = 3600*1000; // in milli-seconds.
		  final long HALFHOUR = 1800*1000;
	        Date d1=new Date(new Date().getTime() +5*HOUR+HALFHOUR + loadShiftHours );
	        SimpleDateFormat dt1 = new SimpleDateFormat("yyyy-MM-dd");
	       //SimpleDateFormat dt1 = new SimpleDateFormat("2018-03-06");
	        System.out.println(dt1.format(d1));
	        SimpleDateFormat dt2 = new SimpleDateFormat("HH:mm");
	       // SimpleDateFormat dt2 = new SimpleDateFormat("22:10");
	        System.out.println(dt2.format(d1));
	        return(compareData(dt1.format(d1),dt2.format(d1)));
	        // return(compareData());
	     
	    }

	   /* static Date toNearestWholeMinute(Date d) {
	        Calendar c = new GregorianCalendar();
	        c.setTime(d);

	        if (c.get(Calendar.SECOND) >= 30)
	            c.add(Calendar.MINUTE, 1);

	        c.set(Calendar.SECOND, 0);

	        return c.getTime();
	    }

	    static Date toNearestWholeHour(Date d) {
	        Calendar c = new GregorianCalendar();
	        c.setTime(d);
	System.out.println(c.get(Calendar.MINUTE));

	        if (c.get(Calendar.MINUTE) >= 30)
	            c.add(Calendar.HOUR,1);

	        c.set(Calendar.MINUTE, 0);
	        c.set(Calendar.SECOND, 0);

	        return c.getTime();
	    }
*/
	    
	    public ArrayList<HashMap<String,Object>> compareData(String date, String time) throws ClassNotFoundException, SQLException
	    
	    {
	    		ScheduleDAO scheduledao=new ScheduleDAO();
	   
	    		ArrayList<HashMap<String,Object>> responseFromSchedules=scheduledao.getEvents( date , time);
	    		 Date d1=new Date();
	  		return responseFromSchedules;
	    	
	    }
	}


