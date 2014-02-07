import com.hp.oo.sdk.content.annotations.Action;
import com.hp.oo.sdk.content.annotations.Output;
import com.hp.oo.sdk.content.annotations.Param;
import com.hp.oo.sdk.content.annotations.Response;
import com.hp.oo.sdk.content.constants.OutputNames;
import com.hp.oo.sdk.content.constants.ResponseNames;
import com.hp.oo.sdk.content.plugin.ActionMetadata.MatchType;
import com.hp.oo.sdk.content.plugin.ActionMetadata.ResponseType;


import java.util.HashMap;
import java.util.Map;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.QueueingConsumer;

public class rmq {

	
	
	@Action(name = "send message",
            description = "sends a message with RabbitMQ",
            outputs = {
                    @Output(OutputNames.RETURN_RESULT),
                    @Output("resultMessage")
            },
            responses = {
                    @Response(text = ResponseNames.SUCCESS, field = OutputNames.RETURN_RESULT, value = "0", matchType = MatchType.COMPARE_GREATER_OR_EQUAL, responseType = ResponseType.RESOLVED),
                    @Response(text = ResponseNames.FAILURE, field = OutputNames.RETURN_RESULT, value = "0", matchType = MatchType.COMPARE_LESS, responseType = ResponseType.ERROR)
			})
	
    public Map<String, String> send(@Param(value = "message") String message,
    								@Param(value = "mqHost", required = true) String mqHost,
    								@Param(value = "mqPort") String mqPortString,
    								@Param(value = "username") String username,
    								@Param(value = "password", encrypted = true) String password,
    								@Param(value = "virtualHost") String virtualHost,
    								@Param(value = "exchange") String exchange,
    								@Param(value = "queueName", required = true) String queueName,
    								@Param(value = "appId") String appId,
    								@Param(value = "clusterId") String clusterId,
    								@Param(value = "contentEncoding") String contentEncoding,
    								@Param(value = "contentType") String contentType,
    								@Param(value = "correlationId") String correlationId,
    								@Param(value = "deliveryMode") String deliveryMode,
    								@Param(value = "expiration") String expiration,
    								@Param(value = "headers") String headers,
    								@Param(value = "messageId") String messageId,
    								@Param(value = "priority") String priority,
    								@Param(value = "replyTo") String replyTo,
    								@Param(value = "timeStamp") String timeStamp,
    								@Param(value = "type") String type,
    								@Param(value = "userId") String userId) 
    								 {
        Map<String, String> resultMap = new HashMap<String, String>();
        AMQP.BasicProperties.Builder bob = new AMQP.BasicProperties.Builder();
        
        if (!appId.isEmpty()) bob = bob.appId(appId);
        if (!clusterId.isEmpty()) bob = bob.clusterId(clusterId);
        if (!contentEncoding.isEmpty()) bob = bob.contentEncoding(contentEncoding);
        if (!contentType.isEmpty()) bob = bob.contentType(contentType);
        if (!correlationId.isEmpty()) bob = bob.correlationId(correlationId);
        // if (!deliveryMode.isEmpty()) bob = bob.deliveryMode(deliveryMode);
        if (!expiration.isEmpty()) bob = bob.expiration(expiration);
        // if (!headers.isEmpty()) bob = bob.headers(headers);
        if (!messageId.isEmpty()) bob = bob.messageId(messageId);
        // if (!priority.isEmpty()) bob = bob.priority(priority);
        if (!replyTo.isEmpty()) bob = bob.replyTo(replyTo);
        // if (!timeStamp.isEmpty()) bob = bob.timeStamp(timeStamp);
        if (!type.isEmpty()) bob = bob.type(type);
        if (!userId.isEmpty()) bob = bob.userId(userId);
        
        AMQP.BasicProperties props = bob.build();
        
        ConnectionFactory factory = new ConnectionFactory();
        setFactory(factory, mqHost, mqPortString, username, password, virtualHost);
        
        try {
        	Connection connection = factory.newConnection();
        	Channel channel = connection.createChannel();
        	
            channel.basicPublish(exchange, queueName, props, message.getBytes());
            
            channel.close();
            connection.close();
        } catch (Exception e) {
        	resultMap.put(OutputNames.RETURN_RESULT, "-1");
        	resultMap.put("resultMessage", "message not sent");
        	return resultMap;
        }
        
        //The "result" output
        resultMap.put(OutputNames.RETURN_RESULT, "0");
        resultMap.put("resultMessage", "message sent");
        return resultMap;
	}
	
	
	
	@Action(name = "retrieve message",
            description = "retrieves a message from RabbitMQ\n"+
            		"\nInputs:\n" +
            		"mqHost: rabbitMQ hostname or ip address\n" +
            		"mqPort: port of the rabbitMQ host, defaults to 5672\n" +
            		"username: to log in to rabbitMQ resp. to virtual host\n" +
            		"password: the password for the given user\n" +
            		"virtualHost: rabbitMQ's virtual host\n" +
            		"queueName: the name of the queue\n" +
            		"autoAck: auto acknowledge messages retireved from the queue " +
            			"(true or false, defaults to false). " +
            			"if autoAck is set not set to true, the message must be acknowledge or rejected later " +
            			"with an accoring step.\n" +
            		"\nOutputs\n:" +
            		"message: the message retrieved from the queue\n" +
            		"deliveryTag: the delivery tag from message envelope\n" +
            		"exchange: exchange from envelope\n" +
            		"routingKey: routingKey from envelope\n" +
            		"isRedilver: is the message redeliverd (causes for redelivery could be that the original " +
            			"cosumer was not available and another cosumer was chosen by rabbitMQ\n",
            outputs = {
                    @Output(OutputNames.RETURN_RESULT),
                    @Output("returnMessage"),
                    @Output("message"),
                    @Output("deliveryTag"),
                    @Output("exchange"),
                    @Output("routingKey"),
                    @Output("isRedeliver")
            },
            responses = {
                    @Response(text = ResponseNames.SUCCESS, field = OutputNames.RETURN_RESULT, value = "0", matchType = MatchType.COMPARE_GREATER_OR_EQUAL, responseType = ResponseType.RESOLVED),
                    @Response(text = ResponseNames.FAILURE, field = OutputNames.RETURN_RESULT, value = "0", matchType = MatchType.COMPARE_LESS, responseType = ResponseType.ERROR)
			})
	
    public Map<String, String> retrieve(@Param(value = "mqHost", required = true) String mqHost,
    									@Param(value = "mqPort") String mqPortString,
    									@Param(value = "username") String username,
    									@Param(value = "password", encrypted = true) String password,
    									@Param(value = "virtualHost") String virtualHost,
    									@Param(value = "queueName", required = true) String queueName,
    									@Param(value = "autoAck") String autoAck) {
        Map<String, String> resultMap = new HashMap<String, String>();
        String message;
        Envelope envelope = new Envelope(0, true, "", "");
         
        try {
        	ConnectionFactory factory = new ConnectionFactory();
        	setFactory(factory, mqHost, mqPortString, username, password, virtualHost);
        	
        	Connection connection = factory.newConnection();
        	Channel channel = connection.createChannel();
        	
        	boolean localAutoAck = false;
        	if (autoAck.equalsIgnoreCase("true")) localAutoAck = true;

        	QueueingConsumer consumer = new QueueingConsumer(channel);
        	channel.basicConsume(queueName, localAutoAck, consumer);
        	
            QueueingConsumer.Delivery delivery = consumer.nextDelivery(1000);
            message = new String(delivery.getBody());
            envelope = delivery.getEnvelope();
            
        } catch (Exception e) {
        	resultMap.put(OutputNames.RETURN_RESULT, "-1");
        	resultMap.put("resultMessage", "could not get message");
        	return resultMap;
        }
        
        resultMap.put(OutputNames.RETURN_RESULT, "0");
        resultMap.put("message", message);
        resultMap.put("deliveryTag", Long.toString(envelope.getDeliveryTag()));
        resultMap.put("exchange", envelope.getExchange());
        resultMap.put("routingKey", envelope.getRoutingKey());
        resultMap.put("isRedeliver", Boolean.toString(envelope.isRedeliver()));
        
        return resultMap;
	}
	
	/**
	 * setFactory sets all parameters if they are given
	 * @param factory
	 * @param mqHost
	 * @param mqPortString
	 * @param username
	 * @param password
	 * @param virtualHost
	 */
	void setFactory(ConnectionFactory factory,
					String mqHost,
					String mqPortString,
					String username,
					String password,
					String virtualHost			
				) {
		factory.setHost(mqHost);
		
		if (!mqPortString.isEmpty() && Integer.parseInt(mqPortString) > 0) {
        	factory.setPort(Integer.parseInt(mqPortString));
        } else {
        	factory.setPort(5672);
        }
		
		if (!username.isEmpty()) {
			factory.setUsername(username);
			if (!password.isEmpty()) {
				factory.setPassword(password);
			}
		}
        
		if (!virtualHost.isEmpty()) {
			factory.setVirtualHost(virtualHost);
		}
	}
}
