import java.util.concurrent.TimeUnit;

public class MissedConnectionsUtil {

	// Helper functions:
	public static boolean missedConnection(long famv, long gamv) {
		// flights sent to this method HAVE to be connecting
		long actualTimeDiff = longTimeDifferenceInMins(famv, gamv);
		if (actualTimeDiff > -30){
			return true;
		}
		return false;
	}
	
	public static boolean isConnection(long famv, long gamv) {
		long timeDiff = longTimeDifferenceInMins(famv, gamv);
		if(timeDiff >= -360 && timeDiff <= -30){
			return true;
		}
		return false;
	}
	
	public static long longTimeDifferenceInMins(long t1, long t2) {
		return TimeUnit.MILLISECONDS.toMinutes(t1 - t2);
	}
	
}
