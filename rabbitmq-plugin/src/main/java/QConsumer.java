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
	private String mqHost = "";
	private Integer mqPort = 8443;
	private String mqUser = "";
	private String mqPass = "";
	private String virtualHost = "";
	private String queueName = "";
	
	public void setOOUser(String user) {
		this.ooUser = user;
	}
	
	public String getOOUser() {
		return this.ooUser;
	}
	
	public void setOOPass(String pass) {
		this.ooPass = pass;
	}
	
	public String getOOPass() {
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
	
	public void setOOHost(String host) {
		this.ooHost = host;
	}
	
	public String getOOHost() {
		return this.ooHost;
	}
	
	public void setOOPort(Integer port) {
		this.ooPort = port;
	}
	
	public void setOOPort(String port) {
		try {
			this.ooPort = Integer.parseInt(port);
		} catch (Exception e) { /* do nothing */ }
	}
	
	public Integer getOOPort() {
		return this.ooPort;
	}
	
	public void setMQHost(String host) {
		this.mqHost = host;
	}
	
	public String getMQHost() {
		return this.mqHost;
	}
	
	public void setMQPort(Integer port) {
		this.mqPort = port;
	}
	
	public void setMQPort(String port) {
		try {
			this.mqPort = Integer.parseInt(port);
		} catch (Exception e) { /* do nothing */ }
	}
	
	public String getMQPort() {
		return this.mqPort.toString();
	}
	
	public void setMQUser(String user) {
		this.mqUser = user;
	}
	
	public String getMQUser() {
		return this.mqUser;
	}
	
	public void setMQPass(String pass) {
		this.mqPass = pass;
	}
	
	public String getMQPass() {
		return this.mqPass;
	}
	
	public void setVirtualHost(String vhost) {
		this.virtualHost = vhost;;
	}
	
	public String getVirtualHost() {
		return this.virtualHost;
	}
	public void setQueueName(String queue) {
		this.queueName = queue;
	}
	
	public String getQueueName() {
		return this.queueName;
	}
}