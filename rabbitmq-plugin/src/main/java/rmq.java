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
	
    public Map<String, String> send(@Param(value = "mqHost", required = true) String mqHost,
    								@Param(value = "mqPort") String mqPortString,
    								@Param(value = "username") String username,
    								@Param(value = "password", encrypted = true) String password,
    								@Param(value = "virtualHost") String virtualHost,
    								@Param(value = "queueName", required = true) String queueName, 
    								@Param(value = "message") String message) {
        Map<String, String> resultMap = new HashMap<String, String>();
        
        ConnectionFactory factory = new ConnectionFactory();
        setFactory(factory, mqHost, mqPortString, username, password, virtualHost);
        
        try {
        	Connection connection = factory.newConnection();
        	Channel channel = connection.createChannel();
        	
            channel.basicPublish("", queueName, null, message.getBytes());
            
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
            description = "retrieves a message from RabbitMQ",
            outputs = {
                    @Output(OutputNames.RETURN_RESULT),
                    @Output("returnMessage"),
                    @Output("message"),
                    @Output("tag"),
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
    									@Param(value = "queueName", required = true) String queueName) {
        Map<String, String> resultMap = new HashMap<String, String>();
        String message;
        Envelope envelope = new Envelope(0, true, "", "");
         
        try {
        	ConnectionFactory factory = new ConnectionFactory();
        	setFactory(factory, mqHost, mqPortString, username, password, virtualHost);
        	
        	Connection connection = factory.newConnection();
        	Channel channel = connection.createChannel();

        	QueueingConsumer consumer = new QueueingConsumer(channel);
        	channel.basicConsume(queueName, true, consumer);
        	
            QueueingConsumer.Delivery delivery = consumer.nextDelivery(1000);
            message = new String(delivery.getBody());
            envelope = delivery.getEnvelope();
            
        } catch (Exception e) {
        	resultMap.put(OutputNames.RETURN_RESULT, "-1");
        	resultMap.put("resultMessage", "could not get message");
        	return resultMap;
        }
        
        //The "result" output
        resultMap.put(OutputNames.RETURN_RESULT, "0");

        //The "result_message" output
        resultMap.put("message", message);
        resultMap.put("envelopeTag", Long.toString(envelope.getDeliveryTag()));
        resultMap.put("envelopeExchange", envelope.getExchange());
        resultMap.put("envelopeRoutingKey", envelope.getRoutingKey());
        resultMap.put("envelopeIsRedeliver", Boolean.toString(envelope.isRedeliver()));
        
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
