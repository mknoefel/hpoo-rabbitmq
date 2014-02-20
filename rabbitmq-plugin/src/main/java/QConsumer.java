import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;


public class QConsumer extends DefaultConsumer {
	
	public QConsumer(Channel channel) { 
		super(channel);
	}
	
	private String ooUser = "";
	private String ooPass = "";
	private String ooFlow = "";
	private String ooRunName = "";
	private String ooHost = "localhost";
	private Integer ooPort = 8443;
	
	public void setUser(String user) {
		this.ooUser = user;
	}
	
	public String getUser() {
		return this.ooUser;
	}
	
	public void setPass(String pass) {
		this.ooPass = pass;
	}
	
	public String getPass() {
		return this.ooPass;
	}
	
	public void setFlow(String flow) {
		this.ooFlow = flow;
	}
	
	public String getFlow() {
		return this.ooFlow;
	}
	
	public void setRunName(String runName) {
		this.ooRunName = runName;
	}
	
	public String getRunName() {
		return this.ooRunName;
	}
	
	public void setHost(String host) {
		this.ooHost = host;
	}
	
	public String getHost() {
		return this.ooHost;
	}
	
	public void setPort(Integer port) {
		this.ooPort = port;
	}
	
	public Integer getPort() {
		return this.ooPort;
	}
	
}