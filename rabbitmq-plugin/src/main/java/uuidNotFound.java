
public class uuidNotFound extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public uuidNotFound () {
		super("requested UUID could not be found on the stack");
	}
	
	public uuidNotFound(String mesg) {
		super("requested UUID could not be found on the stack:"+mesg);
	}
}