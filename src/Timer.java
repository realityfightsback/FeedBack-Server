public class Timer {

	long startTime, endTime;

	public Timer() {
		super();

		startTime = System.currentTimeMillis();

	}

	public long stop() {
		endTime = System.currentTimeMillis();
		return endTime - startTime;
	}

	public void start() {
		startTime = System.currentTimeMillis();
		endTime = -1;
	}

}
