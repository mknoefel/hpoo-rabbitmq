import com.hp.oo.sdk.content.annotations.Action;
import com.hp.oo.sdk.content.annotations.Output;
import com.hp.oo.sdk.content.annotations.Param;
import com.hp.oo.sdk.content.annotations.Response;
import com.hp.oo.sdk.content.constants.OutputNames;
import com.hp.oo.sdk.content.constants.ResponseNames;
import com.hp.oo.sdk.content.plugin.ActionMetadata.MatchType;
import com.hp.oo.sdk.content.plugin.ActionMetadata.ResponseType;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.AMQP.Queue.DeleteOk;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.tools.json.JSONReader;
import com.rabbitmq.tools.json.JSONWriter;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.io.IOException;
import java.text.SimpleDateFormat;

public class rmq {

	/* channelMap holds all active channels */
	private static Map<UUID,Channel> channelMap = new HashMap<UUID,Channel>();
	
	/* 
	 * newUuid generates a new UUID to address a channel. If the UUID is in use already
	 * the function tries to generate a new UUID max. 5 times.
	 */
	private UUID newUuid() {
		UUID uuid = new UUID(0,0);
		boolean uuidOkay;
		
		int i = 0;
		{
			uuidOkay = true;
			uuid = UUID.randomUUID();
			if (channelMap.get(uuid) == null) {
				++i; 
				uuidOkay = false;
			}
		} while (i < 5 && uuidOkay);
		
		return UUID.randomUUID();
	}
	
	/*
	 * returns a channel for a given channelId
	 */
	public Channel getChannel(String channelId) throws NullPointerException {
		UUID uuid = new UUID(0,0);
		Channel channel = null;
		
		if (channelId == null) return null;
		
		try {
			uuid = UUID.fromString(channelId);
			channel = channelMap.get(uuid);
		} catch (Exception e) {
			throw new NullPointerException();
		}
		
		if (channel == null) throw new NullPointerException();
		return channel;
	}
	
	/*
	 * returns a channel for a given channelId
	 */
	public UUID getChannelUuid(String channelId) {
		UUID uuid = new UUID(0,0);
		
		if (channelId == null) return null;
		
		try {
			uuid = UUID.fromString(channelId);
		} catch (Exception e) {
			return null;
		}
		
		return uuid;
	}
	
	/* 
	 * it is possible that the result is null, if defaultValue is null and text does not match any boolean
	 */
	private boolean getBool(String text, boolean defaultValue) { 
		boolean result = defaultValue;
		if (text == null) return defaultValue;
		if (text.equalsIgnoreCase("true".trim())) result = true;
		if (text.equalsIgnoreCase("false".trim())) result = false;
		
		return result;
	}
	
	/*
	 * purge all channels from channelMap when no longer in use
	 */
	private void purgeChannelMap() {
		Integer count = 0;
		
		try {
			for (Map.Entry<UUID, Channel> entry: channelMap.entrySet()) {
				/* if channel is already closed */
				if (!entry.getValue().isOpen()) {
					channelMap.remove(entry.getKey());
					count++;
				}
			} 
		} catch (Exception e) { /* do nothing */ }
	}
	
	private ArrayList<GetResponse> getMessagesByCorrId(String mqHost, String mqPort,
			String mqUser, String mqPass, String virtualHost,
			String queueName, String corrId) {
		ArrayList<GetResponse> mesgs = new ArrayList<GetResponse>();
		Map<String,String> channelResult;
		int mCount; /* messageCount */
		Channel channel;
		
		System.out.println("checking: "+corrId);
		
		/* set up a connection to mq */
		channelResult = createChannel(mqHost, mqPort, virtualHost, mqUser, mqPass);
		if (channelResult.get("channelId") == null) return null;
		channel = getChannel(channelResult.get("channelId"));
		if (channel == null) return null;
		
		/*
		 * loop thru all messages in the queue but do not acknowledge them
		 */
		do {
			GetResponse resp = null;
			try {
				resp = channel.basicGet(queueName, false);
			} catch (Exception e) {
				System.out.println("basicGet: "+e.getMessage());
			}
			
			/* we can exit when there are no more messages */
			if (resp == null) mCount = 0;
			else mCount = resp.getMessageCount();
			
		} while (mCount > 0);

		/* close the connection */
		Connection conn = channel.getConnection();
		try {
			channel.close();
			conn.close();
		} catch (IOException e) {
			/* do nothing */
		}
		
		return mesgs;
	}
	
	@SuppressWarnings("unchecked")
	@Action(name = "send message",
            description = "sends a message with RabbitMQ\n" +
            		"\nInputs:\n" +
            		"message: the message to send\n" +
            		"channelId: the channel to use for sending messages. " +
            			"If the channelId is provided the step will not need " +
            			"'mqHost', 'mqPort', 'username', 'password', and 'virtualHost'.\n" +
            		"closeChannel: shall the channel be closed after this step? True or false, " +
            			"but when this field is empty it will leave the channel open when" +
            			"the channelId was provided otherwise it will close the channel.\n" +
            		"mqHost: FQDN or ip address of the rabbitMQ host\n" +
            		"mqPort: port number of the rabbitMQ host\n" +
            		"username: to log in to rabbitMQ resp. to virtual host\n" +
            		"password: the password for the given user\n" +
            		"virtualHost: rabbitMQ's virtual host\n" +
            		"exchange: exchange to use\n" +
            		"queueName: the name of the queue\n" +
            		"\nEverything that follows now builds the message properties.\n" +
            		"headers: input is a json text with strings, booleans, and integers only " +
            		"(i.e. {\"name\":\"my name\", \"feeling great\": true, \"day of XMAS\": 24)\n" +
            		"timestamp can use the keyword \"now\" to request the current date to be inserted\n" +
            		"dateFormat can be used to describe the format for timestamp " +
            		"(see http://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html)\n" +
            		"\nOutputs:\n" +
            		"channelId: the channelId if left open.\n",
            outputs = {
                    @Output(OutputNames.RETURN_RESULT),
                    @Output("resultMessage"),
                    @Output("channelId")
            },
            responses = {
                    @Response(text = ResponseNames.SUCCESS, field = OutputNames.RETURN_RESULT, value = "0", matchType = MatchType.COMPARE_GREATER_OR_EQUAL, responseType = ResponseType.RESOLVED),
                    @Response(text = ResponseNames.FAILURE, field = OutputNames.RETURN_RESULT, value = "0", matchType = MatchType.COMPARE_LESS, responseType = ResponseType.ERROR)
			})
	public Map<String, String> send(@Param(value = "message") String message,
									@Param(value = "channelId") String channelId,
									@Param(value = "closeChannel") String closeChannel,
    								@Param(value = "mqHost") String mqHost,
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
    								@Param(value = "dateFormat") String dateFormat,
    								@Param(value = "timeStamp") String timeStamp,
    								@Param(value = "type") String type,
    								@Param(value = "userId") String userId) 
    								 {
        Map<String, String> resultMap = new HashMap<String, String>();
        AMQP.BasicProperties.Builder bob = new AMQP.BasicProperties.Builder();
        AMQP.BasicProperties props = null;
         
        /*
         * getting the string from OO we need to check if they are null. Null strings will not
         * work in the code.
         */
        
        if (appId != null && !appId.isEmpty()) bob = bob.appId(appId);
        if (clusterId != null && !clusterId.isEmpty()) bob = bob.clusterId(clusterId);
        if (contentEncoding != null && !contentEncoding.isEmpty()) bob = bob.contentEncoding(contentEncoding);
        if (contentType != null && !contentType.isEmpty()) bob = bob.contentType(contentType);
        if (correlationId != null && !correlationId.isEmpty()) bob = bob.correlationId(correlationId);
        if (deliveryMode != null && !deliveryMode.isEmpty()) bob = bob.deliveryMode(Integer.parseInt(deliveryMode));
        if (expiration != null && !expiration.isEmpty()) bob = bob.expiration(expiration);
        if (headers != null && !headers.isEmpty()) try {
        	Map<String,Object> jason = new HashMap<String,Object>();
        	Map<String,Object> localHeaders = new HashMap<String,Object>();
        	
    		JSONReader rdr = new JSONReader();
    		JSONWriter wtr = new JSONWriter();
    		jason = (Map<String, Object>) rdr.read(headers);
    		
    		for (Map.Entry<String, Object> entry: jason.entrySet()) {
    			String key = entry.getKey();
    	
    			if (entry.getValue() instanceof Integer) {
    				Integer value = (Integer) entry.getValue();
    				localHeaders.put(key, value);
    			}
    			
    			if (entry.getValue() instanceof Boolean) {
    				Boolean value = (Boolean) entry.getValue();
    				localHeaders.put(key, value);
    			}
    			
    			if (entry.getValue() instanceof String) {
    				String value = (String) entry.getValue();
    				localHeaders.put(key, value);
    			}
    			
    			if (entry.getValue() instanceof HashMap) {
    				String value = wtr.write(entry.getValue());
    				localHeaders.put(key, value);
    			}
    			
    			if (entry.getValue() instanceof ArrayList) {
    				String value = wtr.write(entry.getValue());
    				localHeaders.put(key, value);
     			}
    		}
    		bob = bob.headers(localHeaders);
        } catch (Exception e) {
        	resultMap.put(OutputNames.RETURN_RESULT, "-3");
            resultMap.put("resultMessage", "could not read headers");
            return resultMap;
        }
        if (messageId != null && !messageId.isEmpty()) bob = bob.messageId(messageId);
        if (priority != null && !priority.isEmpty()) bob = bob.priority(Integer.parseInt(priority));
        if (replyTo != null && !replyTo.isEmpty()) bob = bob.replyTo(replyTo);
        if (timeStamp != null && !timeStamp.isEmpty()) {
        	
        	if ("now".equals(timeStamp.trim())) {
        		Calendar cal = Calendar.getInstance();  
        		Date now = cal.getTime();
        		bob.timestamp(now);
        	} else { 
        		try {
        			Date localTimeStamp = new SimpleDateFormat(dateFormat).parse(timeStamp);
        			if (timeStamp != null) bob = bob.timestamp(localTimeStamp);
        		} catch (Exception e) {
        			try {
        				Date localTimeStamp = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse(timeStamp);
        				if (timeStamp != null) bob = bob.timestamp(localTimeStamp);
        			} catch (Exception f) {
        				resultMap.put(OutputNames.RETURN_RESULT, "-2");
        				resultMap.put("resultMessage", "inapproriate timestamp");
        				return resultMap;
        			}
        		}
        	}
        }
        	
        if (type != null && !type.isEmpty()) bob = bob.type(type);
        if (userId != null && !userId.isEmpty()) bob = bob.userId(userId);
        
        props = bob.build();
        
        if (channelId == null) channelId = "";
        if (closeChannel == null) closeChannel = "";
        if (mqHost == null) mqHost = "";
        if (mqPortString == null) mqPortString = "";
        if (username == null) username = "";
        if (password == null) password = "";
        if (virtualHost == null) virtualHost = "";
        if (exchange == null) exchange = "";
        if (queueName == null) queueName = "";
        
        
		/*
		 * here we send the message
		 */
        try {
        	Map<String,String> localChannel = new HashMap<String,String>();
        	Channel channel = null;
        	boolean localyCreatedChannel = false;
        	
        	if (channelId != null && !channelId.isEmpty()) channel = getChannel(channelId);
        	if (channel == null) {
        		localChannel = createChannel(mqHost, mqPortString, virtualHost, username, password);
        		channelId = localChannel.get("channelId");
        		channel = getChannel(channelId);
        		localyCreatedChannel = true;
        	}
        	
        	if (channel == null) {
        		resultMap.put(OutputNames.RETURN_RESULT, "-1");
            	resultMap.put("resultMessage", "could not open channel");
            	return resultMap;
        	}
        	
        	channel.basicPublish(exchange, queueName, props, message.getBytes());
        	
            
        	/* if string leaveChannelOpen is explicitly "false" then we close the channel or
        	 * if string leaveChannelOpen is not explicitly true and channel was created in this 
        	 * method then we will close it, because it was not open before!
        	 * 
        	 *  getBool(closeChannel, false):
        	 *     closeChannelOpen == true  ->  result = true
        	 *     closeChannelOpen == false ->  result = false
        	 *     closeChannelOpen == ""    ->  result = false
        	 *     
        	 *  getBool(leaveChannelOpen, true):
        	 *     closeChannelOpen == true  ->  result = true
        	 *     closeChannelOpen == false ->  result = false
        	 *     closeChannelOpen == ""    ->  result = true
        	
        	 */ 
        	if (getBool(closeChannel, false) || (localyCreatedChannel && getBool(closeChannel, true))) {
        		closeChannel("channelId");
        	} else {
        		resultMap.put("channelId", channelId);
        	}
        	
        } catch (NullPointerException e) {
        	resultMap.put(OutputNames.RETURN_RESULT, "-1");
        	resultMap.put("resultMessage", "message not sent: null pointer");
        	return resultMap;
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
            		"channelId: the channel to use to retrieve messages. " +
            			"If the channelId is provided the step will not need " +
            			"'mqHost', 'mqPort', 'username', 'password', and 'virtualHost'.\n" +
            		"closeChannel: shall the channel be closed after this step? True or false, " +
            			"but when this field is empty it will leave the channel open when" +
            			"the channelId was provided otherwise it will close the channel.\n" +
            		"mqHost: FQDN or ip address of the rabbitMQ host\n" +
            		"mqPort: port number of the rabbitMQ host\n" +
            		"username: to log in to rabbitMQ resp. to virtual host\n" +
            		"password: the password for the given user\n" +
            		"virtualHost: rabbitMQ's virtual host\n" +
            		"queueName: the name of the queue\n" +
            		"autoAck: auto acknowledge messages retireved from the queue " +
            			"(true or false, defaults to true). " +
            			"if autoAck is set not set to true, the message must be acknowledge or rejected later " +
            			"with an accoring step.\n" +
            		"dateFormat can be used to describe the timestamp output format " +
                		"(see http://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html)\n" +
            		"\nOutputs:\n" +
            		"message: the message retrieved from the queue\n" +
            		"channelId: the channelId if left open.\n" +
            		"The next 4 outputs represent the message envelope\n" +
            		"deliveryTag: the delivery tag from message envelope\n" +
            		"exchange: exchange from envelope\n" +
            		"routingKey: routingKey from envelope\n" +
            		"isRedilver: is the message redeliverd (causes for redelivery could be that the original " +
            			"cosumer was not available and another cosumer was chosen by rabbitMQ\n" +
            		"The next outputs represent the message properties",
            outputs = {
                    @Output(OutputNames.RETURN_RESULT),
                    @Output("returnMessage"),
                    @Output("channelId"),
                    @Output("messageCount"),
                    @Output("message"),
                    @Output("deliveryTag"),
                    @Output("exchange"),
                    @Output("routingKey"),
                    @Output("isRedeliver"),
                    @Output("appId"), 
                    @Output("clusterId"),
                    @Output("contentEncoding"),
                    @Output("contentType"),
                    @Output("correlationId"),
                    @Output("deliveryMode"),
                    @Output("expiration"),
                    @Output("headers"),
                    @Output("messageId"),
                    @Output("priority"),
                    @Output("replyTo"),
                    @Output("timestamp"),
                    @Output("type"),
                    @Output("userId")
            },
            responses = {
                    @Response(text = "Message Received", field = OutputNames.RETURN_RESULT, value = "0", matchType = MatchType.COMPARE_GREATER, responseType = ResponseType.RESOLVED),
                    @Response(text = "no message available", field = OutputNames.RETURN_RESULT, value = "0", matchType = MatchType.COMPARE_EQUAL, responseType = ResponseType.NO_ACTION_TAKEN),
                    @Response(text = ResponseNames.FAILURE, field = OutputNames.RETURN_RESULT, value = "0", matchType = MatchType.COMPARE_LESS, responseType = ResponseType.ERROR)
			})
	
	public Map<String, String> retrieve(@Param(value = "channelId") String channelId,
			@Param(value = "closeChannel") String closeChannel,
			@Param(value = "mqHost") String mqHost,
			@Param(value = "mqPort") String mqPortString,
			@Param(value = "username") String username,
			@Param(value = "password", encrypted = true) String password,
			@Param(value = "virtualHost") String virtualHost,
			@Param(value = "queueName", required = true) String queueName,
			@Param(value = "autoAck") String autoAck,
			@Param(value = "dateFormat") String dateFormat) {
        Map<String, String> resultMap = new HashMap<String, String>();
        String message = null;
        Envelope envelope = new Envelope(0, true, "", "");
        AMQP.BasicProperties props = new AMQP.BasicProperties();
        GetResponse resp;
        int mesgCount = 0;
        
        if (channelId == null) channelId = "";
        if (closeChannel == null) closeChannel = "";
        if (mqHost == null) mqHost = "";
        if (mqPortString == null) mqPortString = "";
        if (username == null) username = "";
        if (password == null) password = "";
        if (virtualHost == null) virtualHost = "";
        if (queueName == null) queueName = "";
        if (autoAck == null) autoAck = "";
        if (dateFormat == null) dateFormat = "";
        
		/*
		 * next we read the message from the queue
		 */
        Map<String,String> localChannel = new HashMap<String,String>();
        Channel channel = null;
        boolean localyCreatedChannel = false;
        	
        if (channelId != null && !channelId.isEmpty()) {
        	try {
        		channel = getChannel(channelId);
        	} catch (Exception e) {
        		channel = null;
        	}
        }
        		
        if (channel == null) {
        	localChannel = createChannel(mqHost, mqPortString, virtualHost, username, password);
        	try {
        		channelId = localChannel.get("channelId");
        		channel = getChannel(channelId);
        	} catch (Exception e) {
        		resultMap.put(OutputNames.RETURN_RESULT, "-5");
            	resultMap.put("resultMessage", "could not open channel");
            	return resultMap;
        	}
        	localyCreatedChannel = true;
        }
        	
        if (channel == null) {
        	resultMap.put(OutputNames.RETURN_RESULT, "-4");
        	resultMap.put("resultMessage", "could not open channel");
        	return resultMap;
        }
        	
        try {
        	resp = channel.basicGet(queueName, getBool(autoAck, true));
        	message = new String(resp.getBody());
        } catch (Exception e) {
        	resultMap.put(OutputNames.RETURN_RESULT, "0");
        	resultMap.put("resultMessage", "no message available");
        	resultMap.put("channelId", channelId);
        	return resultMap;
        }
        	
        try {
        	envelope = resp.getEnvelope();
        	props = resp.getProps();
        	mesgCount = resp.getMessageCount();
        } catch (Exception e) {
        	resultMap.put(OutputNames.RETURN_RESULT, "-3");
        	resultMap.put("resultMessage", "no message properties available");
        	resultMap.put("channelId", channelId);
        }
        	
        /* if string leaveChannelOpen is explicitly "false" then we close the channel or
         * if string leaveChannelOpen is not explicitly true and channel was created in this 
         * method then we will close it, because it was not open before!
         * 
         *  getBool(closeChannel, false):
         *     closeChannelOpen == true  ->  result = true
         *     closeChannelOpen == false ->  result = false
         *     closeChannelOpen == ""    ->  result = false
         *     
         *  getBool(leaveChannelOpen, true):
         *     closeChannelOpen == true  ->  result = true
         *     closeChannelOpen == false ->  result = false
         *     closeChannelOpen == ""    ->  result = true
         */ 
        if (getBool(closeChannel, false) || (localyCreatedChannel && getBool(closeChannel, true))) {
        	closeChannel("channelId");
        } else {
        	resultMap.put("channelId", channelId);
        }
       
        
        /*
         * we need to create a JSON string for the headers
         */
        JSONWriter wtr = new JSONWriter();
        String headers = wtr.write(props.getHeaders());
        
        /*
         * next we need to define the "timestamp"-string
         */
        String time = "";
        if (props.getTimestamp() != null) {
        	Date timestamp = props.getTimestamp();
        	try {
        		if (dateFormat != null) {
        			time = new SimpleDateFormat(dateFormat).format(timestamp);
        		}
        	} catch (Exception e) {
        		try {
        			time = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(timestamp);
        		} catch (Exception f) {
        			resultMap.put(OutputNames.RETURN_RESULT, "-1");
        			resultMap.put("resultMessage", "inapproriate timestamp");
        			return resultMap;
        		}
        	}
        }
        
        /*
         * creating the step output
         */
        
        resultMap.put(OutputNames.RETURN_RESULT, "1");
        resultMap.put("resultMessage", "message retrieved");
        resultMap.put("messageCount", Integer.toString(mesgCount));
        resultMap.put("message", message);
        resultMap.put("deliveryTag", Long.toString(envelope.getDeliveryTag()));
        resultMap.put("exchange", envelope.getExchange());
        resultMap.put("routingKey", envelope.getRoutingKey());
        resultMap.put("isRedeliver", Boolean.toString(envelope.isRedeliver()));
        resultMap.put("appId", props.getAppId());
        resultMap.put("clusterId", props.getClusterId());
        resultMap.put("contentEncoding", props.getContentEncoding());
        resultMap.put("contentType", props.getContentType());
        resultMap.put("correlationId", props.getCorrelationId());
        if (props.getDeliveryMode() != null) resultMap.put("deliveryMode", Integer.toString(props.getDeliveryMode()));
        resultMap.put("expiration", props.getExpiration());
        resultMap.put("headers", headers);
        resultMap.put("messageId", props.getMessageId());
        if (props.getPriority() != null) resultMap.put("priority", props.getPriority().toString());
        resultMap.put("replyTo", props.getReplyTo());
		resultMap.put("timestamp", time);
		resultMap.put("type", props.getType());
		resultMap.put("userId", props.getUserId()); 
        
        return resultMap;
	}
	
	@Action(name = "acknowledge message",
		description = "acknowledges a message." +
				"\nInputs:\n" +
				"channelId: the channel to use.\n" +
				"closeChannel: shall the channel be closed after this step? True or false.\n" +
				"deliveryTag: indicates the message to acknowledge\n" +
				"multiple: acknowledge mutliple messages (up to this one)?" +
				"True or false, defaults to false.\n" +
				"\nOutput:\n" +
				"channelId: the channelId if left open.\n",
		outputs = {
			@Output(OutputNames.RETURN_RESULT),
			@Output("resultMessage")
		},
		responses = {
            @Response(text = ResponseNames.SUCCESS, field = OutputNames.RETURN_RESULT, value = "0", matchType = MatchType.COMPARE_GREATER_OR_EQUAL, responseType = ResponseType.RESOLVED),
            @Response(text = ResponseNames.FAILURE, field = OutputNames.RETURN_RESULT, value = "0", matchType = MatchType.COMPARE_LESS, responseType = ResponseType.ERROR)
		})
	public Map<String, String> ack(
			@Param(value = "channelId", required = true) String channelId,
			@Param(value = "closeChannel") String closeChannel,
			@Param(value = "deliveryTag", required = true) String deliveryTag,
			@Param(value = "multiple") String multiple) {
		Map<String, String> resultMap = new HashMap<String, String>();
		long localDeliveryTag = 0;
		
		/*
		 * here we send the message
		 */
		Integer err = 0;
        
		Channel channel = null;
        	
		try {	
			if (channelId != null && !channelId.isEmpty()) channel = getChannel(channelId);
		} catch (NullPointerException e) {
			resultMap.put(OutputNames.RETURN_RESULT, "-1");
			resultMap.put("resultMessage", "could not ack: null pointer");
			return resultMap;
		} catch (Exception e) {
			resultMap.put("resultMessage", "an error occured: "+err.toString());
			resultMap.put(OutputNames.RETURN_RESULT, "-1");
			return resultMap;
		}
		
		if (channel == null) {
			resultMap.put(OutputNames.RETURN_RESULT, "-1");
			resultMap.put("resultMessage", "could not get channel");
			return resultMap;
		}
        	
		if (deliveryTag != null) try {
			localDeliveryTag = Long.parseLong(deliveryTag);
		} catch (Exception e) {
			resultMap.put("resultMessage", "something is wrong with the deliveryTag");
			resultMap.put(OutputNames.RETURN_RESULT, "-3");
			return resultMap;
		}
        	
		try {
			channel.basicAck(localDeliveryTag, getBool(multiple, false));
		} catch (Exception e) {
			resultMap.put("resultMessage", "could not acknowledge message");
			resultMap.put(OutputNames.RETURN_RESULT, "-2");
			return resultMap;
		}
        	
		/* if string leaveChannelOpen is explicitly "false" then we close the channel or
		 * if string leaveChannelOpen is not explicitly true and channel was created in this 
		 * method then we will close it, because it was not open before!
		 * 
		 *  getBool(closeChannel, false):
		 *     closeChannelOpen == true  ->  result = true
		 *     closeChannelOpen == false ->  result = false
		 *     closeChannelOpen == ""    ->  result = false
		 *     
		 *  getBool(leaveChannelOpen, true):
		 *     closeChannelOpen == true  ->  result = true
		 *     closeChannelOpen == false ->  result = false
		 *     closeChannelOpen == ""    ->  result = true
        	
		 */ 
		if (getBool(closeChannel, false)) {
			closeChannel(channelId);
		} else {
			resultMap.put("channelId", channelId);
		}
     
		
        resultMap.put("resultMessage", "message(s) ack'ed");
    	resultMap.put(OutputNames.RETURN_RESULT, "0");
        return resultMap;
	}
	
	@Action(name = "reject acknowledge message",
		description = "sends a not 'acknowledged' for a message." +
				"\nInputs:\n" +
				"channelId: the channel to use.\n" +
				"closeChannel: shall the channel be closed after this step? True or false.\n" +
				"deliveryTag: indicates the message to 'not acknowledge'\n" +
				"multiple: 'not acknowledge' mutliple messages (up to this one)?" +
				"True or false, defaults to false.\n" +
				"requeue: shall the messages be requeued? True or false, defaults to false.\n" +
				"\nOutput:\n" +
				"channelId: the channelId if left open.\n",
		outputs = {
			@Output(OutputNames.RETURN_RESULT),
			@Output("resultMessage")
		},
		responses = {
            @Response(text = ResponseNames.SUCCESS, field = OutputNames.RETURN_RESULT, value = "0", matchType = MatchType.COMPARE_GREATER_OR_EQUAL, responseType = ResponseType.RESOLVED),
            @Response(text = ResponseNames.FAILURE, field = OutputNames.RETURN_RESULT, value = "0", matchType = MatchType.COMPARE_LESS, responseType = ResponseType.ERROR)
		})
	public Map<String, String> nAck(
			@Param(value = "channelId", required = true) String channelId,
			@Param(value = "closeChannel") String closeChannel,
			@Param(value = "deliveryTag", required = true) String deliveryTag,
			@Param(value = "multiple") String multiple,
			@Param(value = "requeue") String requeue) {
		Map<String, String> resultMap = new HashMap<String, String>();
		long localDeliveryTag = 0;
		
		/*
		 * here we send nAck
		 */
		Channel channel = null;
        try {
        	if (channelId != null && !channelId.isEmpty()) channel = getChannel(channelId);
        } catch (NullPointerException e) {
        	resultMap.put(OutputNames.RETURN_RESULT, "-1");
        	resultMap.put("resultMessage", "could not nack: null pointer");
        	return resultMap;
        } catch (Exception e) {
        	resultMap.put("resultMessage", "an error occured");
        	resultMap.put(OutputNames.RETURN_RESULT, "-1");
        	return resultMap;
        }
        
        if (channel == null) {
        	resultMap.put(OutputNames.RETURN_RESULT, "-1");
        	resultMap.put("resultMessage", "could not get channel");
        	return resultMap;
        }
        	
        boolean localMultiple = getBool(multiple, false);
        boolean localRequeue = getBool(requeue, false);
        	
        if (deliveryTag != null) try {
        	localDeliveryTag = Long.parseLong(deliveryTag);
        } catch (Exception e) {
        	resultMap.put("resultMessage", "something is wrong with the deliveryTag");
        	resultMap.put(OutputNames.RETURN_RESULT, "-3");
        	return resultMap;
        }
        	
        try {
        	channel.basicNack(localDeliveryTag, localMultiple, localRequeue);
        } catch (Exception e) {
        	resultMap.put("resultMessage", "could not nack message");
        	resultMap.put(OutputNames.RETURN_RESULT, "-2");
        	return resultMap;
        }
        	
        /* if string leaveChannelOpen is explicitly "false" then we close the channel or
         * if string leaveChannelOpen is not explicitly true and channel was created in this 
         * method then we will close it, because it was not open before!
         * 
         *  getBool(closeChannel, false):
         *     closeChannelOpen == true  ->  result = true
         *     closeChannelOpen == false ->  result = false
         *     closeChannelOpen == ""    ->  result = false
         *     
         *  getBool(leaveChannelOpen, true):
         *     closeChannelOpen == true  ->  result = true
         *     closeChannelOpen == false ->  result = false
         *     closeChannelOpen == ""    ->  result = true
        	
         */ 
        if (getBool(closeChannel, false)) {
        	closeChannel(channelId);
        } else {
        	resultMap.put("channelId", channelId);
        }
      
		
        resultMap.put("resultMessage", "nack'ed message");
        resultMap.put(OutputNames.RETURN_RESULT, "0");
		return resultMap;
	}
	
	@Action(name = "recover channel",
			description = "resend unknowledged messages\n" +
					"\nInputs:\n" +
	        		"channelId: the channel to recover.\n" +
	    			"closeChannel: shall the channel be closed after this step? True or false.\n" +
	    			"requeue: shall the messages be requeued? True or false, defaults to false.\n" +
	    			"\nOutput:\n" +
	    			"channelId: the channelId if left open.\n",
			outputs = {
				@Output(OutputNames.RETURN_RESULT),
				@Output("resultMessage")
			},
			responses = {
	            @Response(text = ResponseNames.SUCCESS, field = OutputNames.RETURN_RESULT, value = "0", matchType = MatchType.COMPARE_GREATER_OR_EQUAL, responseType = ResponseType.RESOLVED),
	            @Response(text = ResponseNames.FAILURE, field = OutputNames.RETURN_RESULT, value = "0", matchType = MatchType.COMPARE_LESS, responseType = ResponseType.ERROR)
			})
		public Map<String, String> recover(
				@Param(value = "channelId", required = true) String channelId,
				@Param(value = "closeChannel") String closeChannel,
				@Param(value = "requeue") String requeue) {
			Map<String, String> resultMap = new HashMap<String, String>();
			
			/*
			 * here we recover the channel
			 */
			Channel channel = null;
	        try {
	        	if (channelId != null && !channelId.isEmpty()) channel = getChannel(channelId);
	        } catch (NullPointerException e) {
	        	resultMap.put(OutputNames.RETURN_RESULT, "-1");
	        	resultMap.put("resultMessage", "could not recover: null pointer");
	        	return resultMap;
	        } catch (Exception e) {
	        	resultMap.put("resultMessage", "an error occured");
	        	resultMap.put(OutputNames.RETURN_RESULT, "-1");
	        	return resultMap;
	        }
	   
	        if (channel == null) {
	        	resultMap.put(OutputNames.RETURN_RESULT, "-1");
	        	resultMap.put("resultMessage", "could not get channel");
	        	return resultMap;
	        }
	  
	        boolean localRequeue = false;
	        if (requeue.equalsIgnoreCase("true".trim())) localRequeue = true;
	        
	        try {
	        	channel.basicRecover(localRequeue);
	        } catch (Exception e) {
	        	resultMap.put("resultMessage", "could not recover");
	        	resultMap.put(OutputNames.RETURN_RESULT, "-2");
	        	return resultMap;
	        }
	        	
	        /* if string leaveChannelOpen is explicitly "false" then we close the channel or
	         * if string leaveChannelOpen is not explicitly true and channel was created in this 
	         * method then we will close it, because it was not open before!
	         * 
	         *  getBool(closeChannel, false):
	         *     closeChannelOpen == true  ->  result = true
	         *     closeChannelOpen == false ->  result = false
	         *     closeChannelOpen == ""    ->  result = false
	         *     
	         *  getBool(leaveChannelOpen, true):
	         *     closeChannelOpen == true  ->  result = true
	         *     closeChannelOpen == false ->  result = false
	         *     closeChannelOpen == ""    ->  result = true
	        	
	         */ 
	        if (getBool(closeChannel, false)) {
	        	closeChannel(channelId);
	        } else {
	        	resultMap.put("channelId", channelId);
	        }
	   
			
	        resultMap.put("resultMessage", "channel recovered");
	        resultMap.put(OutputNames.RETURN_RESULT, "0");
			return resultMap;
		}
	
	@Action(name = "create channel",
			description = "creates a channel\n" +
            		"\nInputs:\n" +
            		"mqHost: FQDN or ip address of the rabbitMQ host\n" +
            		"mqPort: port number of the rabbitMQ host\n" +
            		"username: to log in to rabbitMQ resp. to virtual host\n" +
            		"password: the password for the given user\n" +
            		"virtualHost: rabbitMQ's virtual host\n" +
            		"\nOutputs:\n" +
            		"channelId: the newly created channelId.\n",
			outputs = {
				@Output(OutputNames.RETURN_RESULT),
				@Output("resultMessage"),
				@Output("channelId")
			},
			responses = {
	            @Response(text = ResponseNames.SUCCESS, field = OutputNames.RETURN_RESULT, value = "0", matchType = MatchType.COMPARE_GREATER_OR_EQUAL, responseType = ResponseType.RESOLVED),
	            @Response(text = ResponseNames.FAILURE, field = OutputNames.RETURN_RESULT, value = "0", matchType = MatchType.COMPARE_LESS, responseType = ResponseType.ERROR)
			})
		public Map<String, String> createChannel(
				@Param(value = "mqHost", required = true) String mqHost,
				@Param(value = "mqPort") String mqPortString,
				@Param(value = "virtualHost") String virtualHost,
				@Param(value = "username") String username,
				@Param(value = "password", encrypted = true) String password) {
			Map<String, String> resultMap = new HashMap<String, String>();
			ConnectionFactory factory = new ConnectionFactory();
			UUID uuid = new UUID(0,0);
			
			/*
			 * as a kind of housekeeping we purge all channels from the channelMap
			 * that still have an entry but the connection is already closed in
			 * RabbitMQ.
			 */
			purgeChannelMap();
        	
			if (mqHost == null) mqHost = "";
	        if (mqPortString == null) mqPortString = "";
	        if (username == null) username = "";
	        if (password == null) password = "";
	        if (virtualHost == null) virtualHost = "";
	        
			if (mqHost == null || mqHost.isEmpty()) {
				resultMap.put("resultMessage", "mqHost not found");
	        	resultMap.put(OutputNames.RETURN_RESULT, "-1");
	        	return resultMap;
			}
			
			factory.setHost(mqHost);
			        
			if (mqPortString != null && !mqPortString.isEmpty() && Integer.parseInt(mqPortString) > 0) {
	        	factory.setPort(Integer.parseInt(mqPortString));
	        } else {
	        	factory.setPort(5672);
	        }
			
			if (username != null && !username.isEmpty()) {
				factory.setUsername(username);
				if (password != null && !password.isEmpty()) {
					factory.setPassword(password);
				}
			}
	        
			if (virtualHost != null && !virtualHost.isEmpty()) {
				factory.setVirtualHost(virtualHost);
			}
	        
			/*
			 * here we create the channel
			 */
	        try {
	        	Connection connection = factory.newConnection();
	        	Channel channel = connection.createChannel();
	        	
	        	uuid = newUuid();
	        	
	        	if (uuid == null) {
	        		resultMap.put("resultMessage", "could not get new UUID");
		        	resultMap.put(OutputNames.RETURN_RESULT, "-1");
		        	return resultMap;
	        	} else {
	        		channelMap.put(uuid,channel);
	        	}
	        	
	        } catch (Exception e) {
	        	resultMap.put("resultMessage", "an error occured");
	        	resultMap.put(OutputNames.RETURN_RESULT, "-1");
	        	return resultMap;
	        }
			
	        resultMap.put(OutputNames.RETURN_RESULT, "0");
	        resultMap.put("resultMessage", "channel created");
	        resultMap.put("channelId", uuid.toString());
			return resultMap;
		}
	
	@Action(name = "close channel",
			description = "closes a channel in RabbitMQ\n" +
            		"\nInputs:\n" +
            		"channelId: the channel to close\n. ",
			outputs = {
				@Output(OutputNames.RETURN_RESULT),
				@Output("resultMessage"),
				@Output("channelId")
			},
			responses = {
	            @Response(text = ResponseNames.SUCCESS, field = OutputNames.RETURN_RESULT, value = "0", matchType = MatchType.COMPARE_GREATER_OR_EQUAL, responseType = ResponseType.RESOLVED),
	            @Response(text = ResponseNames.FAILURE, field = OutputNames.RETURN_RESULT, value = "0", matchType = MatchType.COMPARE_LESS, responseType = ResponseType.ERROR)
			})
	public Map<String, String> closeChannel(
			@Param(value = "channelId", required = true) String channelId) {
		Map<String, String> resultMap = new HashMap<String, String>();
		
		/*
		 * as a kind of housekeeping we purge all channels from the channelMap
		 * that still have an entry but the connection is already closed in
		 * RabbitMQ.
		 */
		purgeChannelMap();
			
		try {
			Channel channel = getChannel(channelId);
			if (channel == null) {
				resultMap.put("resultMessage", "could not find channel");
				resultMap.put(OutputNames.RETURN_RESULT, "-1");
				return resultMap;
			}
	        	
			Connection con = channel.getConnection();
	        	
			channel.close();
			con.close();
	        	
			channelMap.remove(getChannelUuid(channelId));
		} catch (NullPointerException e) {
			resultMap.put(OutputNames.RETURN_RESULT, "-1");
			resultMap.put("resultMessage", "could not close channel: null pointer");
			return resultMap;
		} catch (Exception e) {
			resultMap.put("resultMessage", "could not close channel");
			resultMap.put(OutputNames.RETURN_RESULT, "-1");
			return resultMap;
		}
			
		resultMap.put(OutputNames.RETURN_RESULT, "0");
		resultMap.put("resultMessage", "channel closed");
		return resultMap;
	}
	
	
	@Action(name = "create temporary queue",
		description ="creates a temporary queue\n" +
				"this queue will be deleted automatically when the channel closes.\n" +
				"\nInput:\n" +
				"channelId: the channel use\n" +
				"\nOutput:\n" +
				"queueName: the name of the newly created queue\n",
		outputs = {
			@Output(OutputNames.RETURN_RESULT),
			@Output("resultMessage"),
			@Output("queueName")
		},
		responses = {
            @Response(text = ResponseNames.SUCCESS, field = OutputNames.RETURN_RESULT, value = "0", matchType = MatchType.COMPARE_GREATER_OR_EQUAL, responseType = ResponseType.RESOLVED),
            @Response(text = ResponseNames.FAILURE, field = OutputNames.RETURN_RESULT, value = "0", matchType = MatchType.COMPARE_LESS, responseType = ResponseType.ERROR)
		}
	)
	public Map<String,String> createTempQueue(@Param(value = "channelId", required = true) String channelId) {
		Map <String,String> resultMap = new HashMap<String,String>();
		Channel channel = null;
		DeclareOk queue;
		
		try {
        	if (channelId != null && !channelId.isEmpty()) channel = getChannel(channelId);
		} catch (Exception e) {
			resultMap.put(OutputNames.RETURN_RESULT, "-3");
        	resultMap.put("resultMessage", "could not get channel");
        	return resultMap;
		}
		
		if (channel == null) {
			resultMap.put(OutputNames.RETURN_RESULT, "-2");
			resultMap.put("resultMessage", "channel is null");
			return resultMap;
		}
        	
		try {
			queue = channel.queueDeclare();
		} catch (Exception e) {
			resultMap.put(OutputNames.RETURN_RESULT, "-1");
			resultMap.put("resultMessage", "could not create temp. queue");
			return resultMap;
		}
        	
		
        	
		resultMap.put(OutputNames.RETURN_RESULT, "0");
    	resultMap.put("resultMessage", "temporary queue declared");
    	resultMap.put("queueName", queue.getQueue());
		return resultMap;
	}
	
	@Action(name = "create queue passively",
			description ="creates a queue if it not already exists\n" +
					"\nInput:\n" +
					"channelId: the channel to be used\n" +
					"queueName: the name of the queue" +
					"\nOutput:\n" +
					"queueName: the name of the newly created queue" +
					"messageCount: the messages currently in the queue" +
					"consumerCount: number of cosumers attached to this queue.",
			outputs = {
			@Output(OutputNames.RETURN_RESULT),
			@Output("resultMessage"),
			@Output("queueName"),
			@Output("messageCount"),
			@Output("consumerCount")
		},
		responses = {
            @Response(text = ResponseNames.SUCCESS, field = OutputNames.RETURN_RESULT, value = "0", matchType = MatchType.COMPARE_GREATER_OR_EQUAL, responseType = ResponseType.RESOLVED),
            @Response(text = ResponseNames.FAILURE, field = OutputNames.RETURN_RESULT, value = "0", matchType = MatchType.COMPARE_LESS, responseType = ResponseType.ERROR)
		}
			)
	public Map<String,String> createPassiveQueue(@Param(value = "channelId", required = true) String channelId,
					@Param(value = "queueName", required = true) String queueName) {
		Map <String,String> resultMap = new HashMap<String,String>();
		Channel channel = null;
		DeclareOk queue;
		Integer messageCount = 0;
		Integer consumerCount = 0;
		
		try {
        	if (channelId != null && !channelId.isEmpty()) channel = getChannel(channelId);
		} catch (Exception e) {
			resultMap.put(OutputNames.RETURN_RESULT, "-1");
        	resultMap.put("resultMessage", "could not get channel");
        	return resultMap;
		}
		
		if (channel == null) {
			resultMap.put(OutputNames.RETURN_RESULT, "-3");
			resultMap.put("resultMessage", "could not get channel");
			return resultMap;
		}
        	
		try {
			queue = channel.queueDeclarePassive(queueName);
			messageCount = queue.getMessageCount();
			consumerCount = queue.getConsumerCount();
		} catch (Exception e) {
			resultMap.put(OutputNames.RETURN_RESULT, "-2");
			resultMap.put("resultMessage", "could not create temp. queue");
			return resultMap;
		}
        	
	
        	
		resultMap.put(OutputNames.RETURN_RESULT, "0");
    	resultMap.put("resultMessage", "queue passively declared");
    	resultMap.put("queueName", queue.getQueue());
    	resultMap.put("messageCount", messageCount.toString());
    	resultMap.put("consumerCount", consumerCount.toString());
		return resultMap;
	}
	
	@SuppressWarnings("unchecked")
	@Action(name = "create queue",
			description ="creates a queue\n" +
					"\n" +
					"\nInput:\n" +
					"channelId: the channel to be used\n" +
					"queueName: the name of the queue\n" +
					"durable: shall the queue survive a reboot? true or false, default to true\n" +
					"exclusive: is this queue bound to the connection? true or false, defaults to false\n" +
					"autoDelete: delete the queue when no longer in use? true or false, defaults to false\n" +
					"\nOutput:\n" +
					"queueName: the name of the newly created queue",
			outputs = {
			@Output(OutputNames.RETURN_RESULT),
			@Output("resultMessage"),
			@Output("queueName")
		},
		responses = {
            @Response(text = ResponseNames.SUCCESS, field = OutputNames.RETURN_RESULT, value = "0", matchType = MatchType.COMPARE_GREATER_OR_EQUAL, responseType = ResponseType.RESOLVED),
            @Response(text = ResponseNames.FAILURE, field = OutputNames.RETURN_RESULT, value = "0", matchType = MatchType.COMPARE_LESS, responseType = ResponseType.ERROR)
		}
			)
	public Map<String,String> createQueue(@Param(value = "channelId", required = true) String channelId,
				@Param(value = "queueName", required = true) String queueName,
				@Param(value = "durable") String durable,
				@Param(value = "exclusive") String exclusive,
				@Param(value = "autoDelete") String autoDelete,
				@Param(value = "arguments") String args) {
		Map <String,String> resultMap = new HashMap<String,String>();
		Map<String,Object> localArgs = new HashMap<String,Object>();
    	Channel channel = null;
		DeclareOk queue;
		Integer messageCount = 0;
		Integer consumerCount = 0;
		
		if (args != null && !args.isEmpty()) try {
        	Map<String,Object> jason = new HashMap<String,Object>();
        	
    		JSONReader rdr = new JSONReader();
    		jason = (Map<String, Object>) rdr.read(args);
    		
    		for (Map.Entry<String, Object> entry: jason.entrySet()) {
    			String key = entry.getKey();
    			
    			if (entry.getValue().getClass().toString().equals("class java.lang.Integer")) {
    				Integer value = (Integer) entry.getValue();
    				localArgs.put(key, value);
    			}
    			
    			if (entry.getValue().getClass().toString().equals("class java.lang.Boolean")) {
    				Boolean value = (Boolean) entry.getValue();
    				localArgs.put(key, value);
    			}
    			
    			if (entry.getValue().getClass().toString().equals("class java.lang.String")) {
    				String value = (String) entry.getValue();
    				localArgs.put(key, value);
    			}
    		}
        } catch (Exception e) {
        	resultMap.put(OutputNames.RETURN_RESULT, "-3");
            resultMap.put("resultMessage", "could not read arguments");
            return resultMap;
        }
		
		if (queueName == null || queueName.isEmpty()) {
			resultMap.put(OutputNames.RETURN_RESULT, "-4");
        	resultMap.put("resultMessage", "queueName not declared");
        	return resultMap;
		}
		
	   	if (channelId != null && !channelId.isEmpty()) try {
    	   		channel = getChannel(channelId);
	   	} catch (Exception e) {
	   		resultMap.put(OutputNames.RETURN_RESULT, "-4");
	   		resultMap.put("resultMessage", "channelId unknown");
	   		return resultMap;
	   	}
        	
        		
	   	if (channel == null) {
	   		resultMap.put(OutputNames.RETURN_RESULT, "-3");
	   		resultMap.put("resultMessage", "could not get channel");
	   		return resultMap;
	   	}
        	
	   	try {
	   		queue = channel.queueDeclare(queueName, getBool(durable, true), 
	   				getBool(exclusive, false), getBool(autoDelete, false), localArgs);
	   		messageCount = queue.getMessageCount();
	   		consumerCount = queue.getConsumerCount();
	   	} catch (Exception e) {
	   		resultMap.put(OutputNames.RETURN_RESULT, "-2");
	   		resultMap.put("resultMessage", "could not create temp. queue");
	   		return resultMap;
	   	}
        	
		        	
		resultMap.put(OutputNames.RETURN_RESULT, "0");
    	resultMap.put("resultMessage", "queue declared");
    	resultMap.put("queueName", queue.getQueue());
    	resultMap.put("messageCount", messageCount.toString());
    	resultMap.put("consumerCount", consumerCount.toString());
    	return resultMap;
	}
	
	@Action(name = "delete queue",
			description ="deletes a queue\n" +
					"\nInput:\n" +
					"channelId: the channel to be used\n" +
					"queueName: the name of the queue" +
					"ifUnused: delete the queue only when not in use? True or false, defaults to true\n" +
					"ifEmpty: delete the queue only when it is empty? True or false, defaults to true\n" +
					"\nOutput:\n" +
					"messageCount: the messages currently in the queue" +
					"consumerCount: number of cosumers attached to this queue.",
			outputs = {
			@Output(OutputNames.RETURN_RESULT),
			@Output("resultMessage"),
			@Output("queueName")
		},
		responses = {
            @Response(text = ResponseNames.SUCCESS, field = OutputNames.RETURN_RESULT, value = "0", matchType = MatchType.COMPARE_GREATER_OR_EQUAL, responseType = ResponseType.RESOLVED),
            @Response(text = ResponseNames.FAILURE, field = OutputNames.RETURN_RESULT, value = "0", matchType = MatchType.COMPARE_LESS, responseType = ResponseType.ERROR)
		}
			)
	public Map<String,String> deleteQueue(@Param(value = "channelId", required = true) String channelId,
					@Param(value = "queueName", required = true) String queueName,
					@Param(value = "ifUnused") String ifUnused,
					@Param(value = "ifEmpty") String ifEmpty) {
		Map <String,String> resultMap = new HashMap<String,String>();
		Channel channel = null;
		DeleteOk queue;
		Integer messageCount = 0;
		Integer consumerCount = 0;
		
		try {
        	if (channelId != null && !channelId.isEmpty()) channel = getChannel(channelId);
        	if (channel == null) throw new IOException();
		} catch (Exception e) {
			resultMap.put(OutputNames.RETURN_RESULT, "-1");
        	resultMap.put("resultMessage", "could not get channel");
        	return resultMap;
		}
		
		try {
			queue = channel.queueDelete(queueName, getBool(ifUnused, true), getBool(ifEmpty, true));
			messageCount = queue.getMessageCount();
		} catch (Exception e) {
			e.printStackTrace();
			resultMap.put(OutputNames.RETURN_RESULT, "-2");
			resultMap.put("resultMessage", "could not delete queue");
			return resultMap;
		}
        	
		resultMap.put(OutputNames.RETURN_RESULT, "0");
    	resultMap.put("resultMessage", "queue deleted");
    	resultMap.put("messageCount", messageCount.toString());
    	resultMap.put("consumerCount", consumerCount.toString());
		return resultMap;
	}
	
	@SuppressWarnings("unchecked")
	@Action(name = "create exchange",
			description ="creates an exchange\n" +
					"\nInput:\n" +
					"channelId: the channel to be used\n" +
					"exchange: the name of the exchange\n" +
					"durable: shall the exchange survive a reboot? true or false, default to true\n" +
					"internal: is the exchange internal, i.e. can't be directly published to by a client.? true or false, defaults to false\n" +
					"autoDelete: delete the exchange when no longer in use? true or false, defaults to false\n",
			outputs = {
			@Output(OutputNames.RETURN_RESULT),
			@Output("resultMessage"),
			@Output("queueName")
		},
		responses = {
            @Response(text = ResponseNames.SUCCESS, field = OutputNames.RETURN_RESULT, value = "0", matchType = MatchType.COMPARE_GREATER_OR_EQUAL, responseType = ResponseType.RESOLVED),
            @Response(text = ResponseNames.FAILURE, field = OutputNames.RETURN_RESULT, value = "0", matchType = MatchType.COMPARE_LESS, responseType = ResponseType.ERROR)
		}
			)
	public Map<String,String> createExchange(@Param(value = "channelId", required = true) String channelId,
				@Param(value = "exchange", required = true) String exchange,
				@Param(value = "type") String etype,
				@Param(value = "durable") String durable,
				@Param(value = "autoDelete") String autoDelete,
				@Param(value = "internal") String internal,
				@Param(value = "arguments") String args) {
		Map <String,String> resultMap = new HashMap<String,String>();
		Map<String,Object> localArgs = new HashMap<String,Object>();
    	Channel channel = null;
    		
		if (args != null && !args.isEmpty()) try {
        	Map<String,Object> jason = new HashMap<String,Object>();
        	
    		JSONReader rdr = new JSONReader();
    		jason = (Map<String, Object>) rdr.read(args);
    		
    		for (Map.Entry<String, Object> entry: jason.entrySet()) {
    			String key = entry.getKey();
    			
    			if (entry.getValue().getClass().toString().equals("class java.lang.Integer")) {
    				Integer value = (Integer) entry.getValue();
    				localArgs.put(key, value);
    			}
    			
    			if (entry.getValue().getClass().toString().equals("class java.lang.Boolean")) {
    				Boolean value = (Boolean) entry.getValue();
    				localArgs.put(key, value);
    			}
    			
    			if (entry.getValue().getClass().toString().equals("class java.lang.String")) {
    				String value = (String) entry.getValue();
    				localArgs.put(key, value);
    			}
    		}
        } catch (Exception e) {
        	resultMap.put(OutputNames.RETURN_RESULT, "-3");
            resultMap.put("resultMessage", "could not read arguments");
            return resultMap;
        }
		
		if (exchange == null || exchange.isEmpty()) {
			resultMap.put(OutputNames.RETURN_RESULT, "-4");
        	resultMap.put("resultMessage", "exchange not declared");
        	return resultMap;
		}
		
		if (etype == null || etype.isEmpty()) etype = "direct";
		
		if (channelId != null && !channelId.isEmpty()) try {
			channel = getChannel(channelId);
		} catch (Exception e) {
			resultMap.put(OutputNames.RETURN_RESULT, "-4");
			resultMap.put("resultMessage", "channelId unknown");
			return resultMap;
		}
        	
        		
		if (channel == null) {
			resultMap.put(OutputNames.RETURN_RESULT, "-3");
			resultMap.put("resultMessage", "could not get channel");
			return resultMap;
		}
        	
		try {
			channel.exchangeDeclare(exchange, etype, getBool(durable, true), 
					getBool(autoDelete, false), getBool(internal, false), localArgs);
		} catch (Exception e) {
			resultMap.put(OutputNames.RETURN_RESULT, "-2");
			resultMap.put("resultMessage", "could not create exchange");
			return resultMap;
		}
        	
		resultMap.put(OutputNames.RETURN_RESULT, "0");
    	resultMap.put("resultMessage", "exchange declared");
    	return resultMap;
	}
	
	@Action(name = "delete exchange",
			description ="deletes an exchange\n" +
					"\nInput:\n" +
					"channelId: the channel to use\n" +
					"exchange: the name of the exchange" +
					"ifUnused: delete the exchange only when not in use? True or false, defaults to true" +
					"\nOutput:\n" +
					"queueName: the name of the newly created queue" +
					"messageCount: the messages currently in the queue" +
					"consumerCount: number of cosumers attached to this queue.",
			outputs = {
			@Output(OutputNames.RETURN_RESULT),
			@Output("resultMessage"),
			@Output("queueName")
		},
		responses = {
            @Response(text = ResponseNames.SUCCESS, field = OutputNames.RETURN_RESULT, value = "0", matchType = MatchType.COMPARE_GREATER_OR_EQUAL, responseType = ResponseType.RESOLVED),
            @Response(text = ResponseNames.FAILURE, field = OutputNames.RETURN_RESULT, value = "0", matchType = MatchType.COMPARE_LESS, responseType = ResponseType.ERROR)
		}
			)
	public Map<String,String> deleteExchange(@Param(value = "channelId", required = true) String channelId,
					@Param(value = "exchange", required = true) String exchange,
					@Param(value = "ifUnused") String ifUnused) {
		Map <String,String> resultMap = new HashMap<String,String>();
		Channel channel = null;
		Integer messageCount = 0;
		Integer consumerCount = 0;
		
		try {
        	if (channelId != null && !channelId.isEmpty()) channel = getChannel(channelId);
        	if (channel == null) throw new IOException();
		} catch (Exception e) {
			resultMap.put(OutputNames.RETURN_RESULT, "-1");
        	resultMap.put("resultMessage", "could not get channel");
        	return resultMap;
		}
	   	
		try {
			channel.exchangeDelete(exchange, getBool(ifUnused, true));
		} catch (Exception e) {
			e.printStackTrace();
			resultMap.put(OutputNames.RETURN_RESULT, "-2");
			resultMap.put("resultMessage", "could not delete exchange");
			return resultMap;
		}
        	
		resultMap.put(OutputNames.RETURN_RESULT, "0");
    	resultMap.put("resultMessage", "exchange deleted");
    	resultMap.put("messageCount", messageCount.toString());
    	resultMap.put("consumerCount", consumerCount.toString());
		return resultMap;
	}
	
	@SuppressWarnings("unchecked")
	@Action(name = "bind queue",
			description ="binds a queue to an exchange\n" +
					"\nInput:\n" +
					"channelId: the channel to be used\n" +
					"queueName: the name of the queue\n" +
					"durable: shall the queue survive a reboot? true or false, default to true\n" +
					"exclusive: is this queue bound to the connection? true or false, defaults to false\n" +
					"autoDelete: delete the queue when no longer in use? true or false, defaults to false\n" +
					"\nOutput:\n" +
					"queueName: the name of the newly created queue",
			outputs = {
			@Output(OutputNames.RETURN_RESULT),
			@Output("resultMessage"),
			@Output("queueName")
		},
		responses = {
            @Response(text = ResponseNames.SUCCESS, field = OutputNames.RETURN_RESULT, value = "0", matchType = MatchType.COMPARE_GREATER_OR_EQUAL, responseType = ResponseType.RESOLVED),
            @Response(text = ResponseNames.FAILURE, field = OutputNames.RETURN_RESULT, value = "0", matchType = MatchType.COMPARE_LESS, responseType = ResponseType.ERROR)
		}
			)
	public Map<String,String> bindQueue(@Param(value = "channelId", required = true) String channelId,
				@Param(value = "queueName", required = true) String queueName,
				@Param(value = "exchange", required = true) String exchange,
				@Param(value = "routingKey") String routingKey,
				@Param(value = "arguments") String args) {
		Map <String,String> resultMap = new HashMap<String,String>();
		Map<String,Object> localArgs = new HashMap<String,Object>();
    	Channel channel = null;
		
    	
		if (args != null && !args.isEmpty()) try {
        	Map<String,Object> jason = new HashMap<String,Object>();
        	
    		JSONReader rdr = new JSONReader();
    		jason = (Map<String, Object>) rdr.read(args);
    		
    		for (Map.Entry<String, Object> entry: jason.entrySet()) {
    			String key = entry.getKey();
    			
    			if (entry.getValue().getClass().toString().equals("class java.lang.Integer")) {
    				Integer value = (Integer) entry.getValue();
    				localArgs.put(key, value);
    			}
    			
    			if (entry.getValue().getClass().toString().equals("class java.lang.Boolean")) {
    				Boolean value = (Boolean) entry.getValue();
    				localArgs.put(key, value);
    			}
    			
    			if (entry.getValue().getClass().toString().equals("class java.lang.String")) {
    				String value = (String) entry.getValue();
    				localArgs.put(key, value);
    			}
    		}
        } catch (Exception e) {
        	resultMap.put(OutputNames.RETURN_RESULT, "-3");
            resultMap.put("resultMessage", "could not read arguments");
            return resultMap;
        }
		
		if (queueName == null || queueName.isEmpty()) {
			resultMap.put(OutputNames.RETURN_RESULT, "-4");
        	resultMap.put("resultMessage", "queueName not given");
        	return resultMap;
		}
		
		if (exchange == null || exchange.isEmpty()) {
			resultMap.put(OutputNames.RETURN_RESULT, "-4");
        	resultMap.put("resultMessage", "exchange not given");
        	return resultMap;
		}
		
		if (routingKey == null) routingKey = "";
    	
		
		if (channelId != null && !channelId.isEmpty()) try {
			channel = getChannel(channelId);
		} catch (Exception e) {
			resultMap.put(OutputNames.RETURN_RESULT, "-4");
			resultMap.put("resultMessage", "channelId unknown");
			return resultMap;
		}
        	
        		
		if (channel == null) {
			resultMap.put(OutputNames.RETURN_RESULT, "-3");
			resultMap.put("resultMessage", "could not get channel");
			return resultMap;
		}
        	
		try {
			channel.queueBind(queueName, exchange, routingKey, localArgs);
		} catch (Exception e) {
			resultMap.put(OutputNames.RETURN_RESULT, "-2");
			resultMap.put("resultMessage", "could not bind queue to exchange");
			return resultMap;
		}
        	
		resultMap.put(OutputNames.RETURN_RESULT, "0");
    	resultMap.put("resultMessage", "queue bound to exchange");
    	return resultMap;
	}
	
	@SuppressWarnings("unchecked")
	@Action(name = "unbind queue",
			description ="unbinds a queue from an exchange\n" +
					"\nInput:\n" +
					"channelId: the channel to be used\n" +
					"queueName: the name of the queue\n" +
					"exchange: name of the exchange to unbind from\n" +
					"routingKey: the routingKey used for the binding\n" +
					"arguments: arguments to pass (see RabbitMQ documentation for details)\n",
			outputs = {
			@Output(OutputNames.RETURN_RESULT),
			@Output("resultMessage")
		},
		responses = {
            @Response(text = ResponseNames.SUCCESS, field = OutputNames.RETURN_RESULT, value = "0", matchType = MatchType.COMPARE_GREATER_OR_EQUAL, responseType = ResponseType.RESOLVED),
            @Response(text = ResponseNames.FAILURE, field = OutputNames.RETURN_RESULT, value = "0", matchType = MatchType.COMPARE_LESS, responseType = ResponseType.ERROR)
		}
			)
	public Map<String,String> unbindQueue(@Param(value = "channelId", required = true) String channelId,
				@Param(value = "queueName", required = true) String queueName,
				@Param(value = "exchange", required = true) String exchange,
				@Param(value = "routingKey") String routingKey,
				@Param(value = "arguments") String args) {
		Map <String,String> resultMap = new HashMap<String,String>();
		Map<String,Object> localArgs = new HashMap<String,Object>();
    	Channel channel = null;
		
    	
		if (args != null && !args.isEmpty()) try {
        	Map<String,Object> jason = new HashMap<String,Object>();
        	
    		JSONReader rdr = new JSONReader();
    		jason = (Map<String, Object>) rdr.read(args);
    		
    		for (Map.Entry<String, Object> entry: jason.entrySet()) {
    			String key = entry.getKey();
    			
    			if (entry.getValue().getClass().toString().equals("class java.lang.Integer")) {
    				Integer value = (Integer) entry.getValue();
    				localArgs.put(key, value);
    			}
    			
    			if (entry.getValue().getClass().toString().equals("class java.lang.Boolean")) {
    				Boolean value = (Boolean) entry.getValue();
    				localArgs.put(key, value);
    			}
    			
    			if (entry.getValue().getClass().toString().equals("class java.lang.String")) {
    				String value = (String) entry.getValue();
    				localArgs.put(key, value);
    			}
    		}
        } catch (Exception e) {
        	resultMap.put(OutputNames.RETURN_RESULT, "-3");
            resultMap.put("resultMessage", "could not read arguments");
            return resultMap;
        }
		
		if (queueName == null || queueName.isEmpty()) {
			resultMap.put(OutputNames.RETURN_RESULT, "-4");
        	resultMap.put("resultMessage", "queueName not given");
        	return resultMap;
		}
		
		if (exchange == null || exchange.isEmpty()) {
			resultMap.put(OutputNames.RETURN_RESULT, "-4");
        	resultMap.put("resultMessage", "exchange not given");
        	return resultMap;
		}
		
		if (routingKey == null) routingKey = "";
    	
		
		if (channelId != null && !channelId.isEmpty()) try {
			channel = getChannel(channelId);
		} catch (Exception e) {
			resultMap.put(OutputNames.RETURN_RESULT, "-4");
			resultMap.put("resultMessage", "channelId unknown");
			return resultMap;
		}
        	
        		
		if (channel == null) {
			resultMap.put(OutputNames.RETURN_RESULT, "-3");
			resultMap.put("resultMessage", "could not get channel");
			return resultMap;
		}
        	
		try {
			channel.queueUnbind(queueName, exchange, routingKey, localArgs);
		} catch (Exception e) {
			resultMap.put(OutputNames.RETURN_RESULT, "-2");
			resultMap.put("resultMessage", "could not unbind queue to exchange");
			return resultMap;
		}
        	
		resultMap.put(OutputNames.RETURN_RESULT, "0");
    	resultMap.put("resultMessage", "queue unbound from exchange");
    	return resultMap;
	}
	
	@SuppressWarnings("unchecked")
	@Action(name = "bind exchange",
		description ="binds an exchange to another exchange\n" +
				"\nInput:\n" +
				"channelId: the channel to be used\n" +
				"destination: name of the destination exchange\n" +
				"source: the name of the source exchange\n" +
				"routingKey: the routingKey used for the binding\n" +
				"arguments: arguments to pass (see RabbitMQ documentation for details)\n",
		outputs = {
			@Output(OutputNames.RETURN_RESULT),
			@Output("resultMessage")
		},
		responses = {
            @Response(text = ResponseNames.SUCCESS, field = OutputNames.RETURN_RESULT, value = "0", matchType = MatchType.COMPARE_GREATER_OR_EQUAL, responseType = ResponseType.RESOLVED),
            @Response(text = ResponseNames.FAILURE, field = OutputNames.RETURN_RESULT, value = "0", matchType = MatchType.COMPARE_LESS, responseType = ResponseType.ERROR)
		}
			)
	public Map<String,String> bindExchange(@Param(value = "channelId", required = true) String channelId,
				@Param(value = "destination", required = true) String destination,
				@Param(value = "source", required = true) String source,
				@Param(value = "routingKey") String routingKey,
				@Param(value = "arguments") String args) {
		Map <String,String> resultMap = new HashMap<String,String>();
		Map<String,Object> localArgs = new HashMap<String,Object>();
    	Channel channel = null;
		
    	
		if (args != null && !args.isEmpty()) try {
        	Map<String,Object> jason = new HashMap<String,Object>();
        	
    		JSONReader rdr = new JSONReader();
    		jason = (Map<String, Object>) rdr.read(args);
    		
    		for (Map.Entry<String, Object> entry: jason.entrySet()) {
    			String key = entry.getKey();
    			
    			if (entry.getValue().getClass().toString().equals("class java.lang.Integer")) {
    				Integer value = (Integer) entry.getValue();
    				localArgs.put(key, value);
    			}
    			
    			if (entry.getValue().getClass().toString().equals("class java.lang.Boolean")) {
    				Boolean value = (Boolean) entry.getValue();
    				localArgs.put(key, value);
    			}
    			
    			if (entry.getValue().getClass().toString().equals("class java.lang.String")) {
    				String value = (String) entry.getValue();
    				localArgs.put(key, value);
    			}
    		}
        } catch (Exception e) {
        	resultMap.put(OutputNames.RETURN_RESULT, "-3");
            resultMap.put("resultMessage", "could not read arguments");
            return resultMap;
        }
		
		if (destination == null || destination.isEmpty()) {
			resultMap.put(OutputNames.RETURN_RESULT, "-4");
        	resultMap.put("resultMessage", "destination not given");
        	return resultMap;
		}
		
		if (source == null || source.isEmpty()) {
			resultMap.put(OutputNames.RETURN_RESULT, "-4");
        	resultMap.put("resultMessage", "source not given");
        	return resultMap;
		}
		
		if (routingKey == null) routingKey = "";
    	
		
		if (channelId != null && !channelId.isEmpty()) try {
			channel = getChannel(channelId);
		} catch (Exception e) {
			resultMap.put(OutputNames.RETURN_RESULT, "-4");
			resultMap.put("resultMessage", "channelId unknown");
			return resultMap;
		}
        	
        		
		if (channel == null) {
			resultMap.put(OutputNames.RETURN_RESULT, "-3");
			resultMap.put("resultMessage", "could not get channel");
			return resultMap;
		}
        	
		try {
			channel.exchangeBind(destination, source, routingKey, localArgs);
		} catch (Exception e) {
			resultMap.put(OutputNames.RETURN_RESULT, "-2");
			resultMap.put("resultMessage", "could not bind source to destination exchange");
			return resultMap;
		}
        	
		resultMap.put(OutputNames.RETURN_RESULT, "0");
    	resultMap.put("resultMessage", "source exchange bound to dest. exchange");
    	return resultMap;
	}
	
	@SuppressWarnings("unchecked")
	@Action(name = "unbind exchange",
			description ="unbinds an exchange from another exchange\n" +
					"\nInput:\n" +
					"channelId: the channel to be used\n" +
					"destination: the name of the destination exchange\n" +
					"source: name of the source exchange\n" +
					"routingKey: the routingKey used for the binding\n" +
					"arguments: arguments to pass (see RabbitMQ documentation for details)\n",
		outputs = {
			@Output(OutputNames.RETURN_RESULT),
			@Output("resultMessage")
		},
		responses = {
            @Response(text = ResponseNames.SUCCESS, field = OutputNames.RETURN_RESULT, value = "0", matchType = MatchType.COMPARE_GREATER_OR_EQUAL, responseType = ResponseType.RESOLVED),
            @Response(text = ResponseNames.FAILURE, field = OutputNames.RETURN_RESULT, value = "0", matchType = MatchType.COMPARE_LESS, responseType = ResponseType.ERROR)
		}
			)
	public Map<String,String> unbindExchange(@Param(value = "channelId", required = true) String channelId,
				@Param(value = "destination", required = true) String destination,
				@Param(value = "source", required = true) String source,
				@Param(value = "routingKey") String routingKey,
				@Param(value = "arguments") String args) {
		Map <String,String> resultMap = new HashMap<String,String>();
		Map<String,Object> localArgs = new HashMap<String,Object>();
    	Channel channel = null;
		
    	
		if (args != null && !args.isEmpty()) try {
        	Map<String,Object> jason = new HashMap<String,Object>();
        	
    		JSONReader rdr = new JSONReader();
    		jason = (Map<String, Object>) rdr.read(args);
    		
    		for (Map.Entry<String, Object> entry: jason.entrySet()) {
    			String key = entry.getKey();
    			
    			if (entry.getValue().getClass().toString().equals("class java.lang.Integer")) {
    				Integer value = (Integer) entry.getValue();
    				localArgs.put(key, value);
    			}
    			
    			if (entry.getValue().getClass().toString().equals("class java.lang.Boolean")) {
    				Boolean value = (Boolean) entry.getValue();
    				localArgs.put(key, value);
    			}
    			
    			if (entry.getValue().getClass().toString().equals("class java.lang.String")) {
    				String value = (String) entry.getValue();
    				localArgs.put(key, value);
    			}
    		}
        } catch (Exception e) {
        	resultMap.put(OutputNames.RETURN_RESULT, "-3");
            resultMap.put("resultMessage", "could not read arguments");
            return resultMap;
        }
		
		if (destination == null || destination.isEmpty()) {
			resultMap.put(OutputNames.RETURN_RESULT, "-4");
        	resultMap.put("resultMessage", "destination not given");
        	return resultMap;
		}
		
		if (source == null || source.isEmpty()) {
			resultMap.put(OutputNames.RETURN_RESULT, "-4");
        	resultMap.put("resultMessage", "source not given");
        	return resultMap;
		}
		
		if (routingKey == null) routingKey = "";
    	
		
		if (channelId != null && !channelId.isEmpty()) try {
			channel = getChannel(channelId);
		} catch (Exception e) {
			resultMap.put(OutputNames.RETURN_RESULT, "-4");
			resultMap.put("resultMessage", "channelId unknown");
			return resultMap;
		}
        	
        		
		if (channel == null) {
			resultMap.put(OutputNames.RETURN_RESULT, "-3");
			resultMap.put("resultMessage", "could not get channel");
			return resultMap;
		}
        	
		try {
			channel.exchangeUnbind(destination, source, routingKey, localArgs);
		} catch (Exception e) {
			resultMap.put(OutputNames.RETURN_RESULT, "-2");
			resultMap.put("resultMessage", "could not unbind exchange from exchange");
			return resultMap;
		}
        	
		resultMap.put(OutputNames.RETURN_RESULT, "0");
    	resultMap.put("resultMessage", "source exchange unbound from dest. exchange");
    	return resultMap;
	}
	

	@SuppressWarnings("unchecked")
	@Action(name = "create consumer",
            description = "creates a consumer in RabbitMQ\n" +
            		"When a message arrives at this consumer the step kicks off a new OO flow. " +
            		"The input fields for the flow must be given in json format in the header " +
            			"with the header field 'inputs', " +
            			"i.e. {\"input1\":\"value1\",\"input2\":\"value2\"}.\n" +
            			"Since this step makes use the OO REST API it needs a compatible cacert in the " +
            			"cacerts keystore in your Java installation (JRE_HOME/lib/security/cacerts).\n" +
            		"\nInputs:\n" +
            		"channelId: if a channel is already open it can be passed to this step " +
            			"and a new channel will not be open (although credentials might " +
            			"be given). The channelId is provided by this step or by createChannel.\n" +
            		"mqHost: FQDN or ip address of the rabbitMQ host\n" +
            		"mqPort: port number of the rabbitMQ host\n" +
            		"username: to log in to rabbitMQ resp. to virtual host\n" +
            		"password: the password for the given user\n" +
            		"virtualHost: rabbitMQ's virtual host\n" +
            		"queueName: the queue to attach consumer to." +
            		"flowUuid: the uuid of the flow in OO\n" +
            		"runName: the name the flow as it appears in the reports (not the flow path). " +
            			"The runName defaults to the consumerTag and looks like amq.gen-...\n" +
            		"ooHost: the OO host to connect to\n" +
            		"ooPort: the port of the OO host, defaults to 8443\n" +
            		"ooUsername: the username the flow shall be started with in OO\n" +
            		"ooPassword: the corresponding password\n" +
            		"exclusive: shall the consumer be exclusive? Defaults to true." +
            		"\nOutputs:\n" +
            		"channelId: the id of the channel used.\n",
            outputs = {
                    @Output(OutputNames.RETURN_RESULT),
                    @Output("resultMessage"),
                    @Output("channelId")
            },
            responses = {
                    @Response(text = ResponseNames.SUCCESS, field = OutputNames.RETURN_RESULT, value = "0", matchType = MatchType.COMPARE_GREATER_OR_EQUAL, responseType = ResponseType.RESOLVED),
                    @Response(text = ResponseNames.FAILURE, field = OutputNames.RETURN_RESULT, value = "0", matchType = MatchType.COMPARE_LESS, responseType = ResponseType.ERROR)
			})
	public Map<String,String> createConsumer(
			@Param(value = "channelId") String channelId,
			@Param(value = "mqHost") String mqHost,
			@Param(value = "mqPort") String mqPortString,
			@Param(value = "username") String username,
			@Param(value = "password", encrypted = true) String password,
			@Param(value = "virtualHost") String virtualHost,
			@Param(value = "queueName") String queueName,
			@Param(value = "flowUuid", required = true) String flowUuid,
			@Param(value = "runName") String runName,
			@Param(value = "ooHost") String ooHost,
			@Param(value = "ooPort") String ooPortString,
			@Param(value = "ooUsername") String ooUsername,
			@Param(value = "ooPassword", encrypted = true) String ooPassword,
			@Param(value = "exclusive") String exclusive,
			@Param(value = "arguments") String args) {
		Map<String,String> resultMap = new HashMap<String,String>();
		Map<String,String> localChannel = new HashMap<String,String>();
		Map<String,Object> localArgs = new HashMap<String,Object>();
    	Integer ooPort;
		String consumerTag = "";
    	Channel channel = null;
    	
    	
    	/*
    	 * check all inputs are not null (channelId follows)
    	 */
    	
    	if (mqHost == null) mqHost = "";
    	if (mqPortString == null) mqPortString = "";
    	if (username == null) username = "";
    	if (password == null) password = "";
    	if (virtualHost == null) virtualHost = "";
    	if (queueName == null) queueName = "";
    	if (flowUuid == null) {
    		resultMap.put(OutputNames.RETURN_RESULT, "-3");
            resultMap.put("resultMessage", "no flow uuid given");
            return resultMap;
    	}
    	if (runName == null) runName = "";
    	if (exclusive == null) exclusive = "";
    	if (ooHost == null) ooHost = "localhost";
    	if (ooPortString == null) {
    		ooPort = 8443;
    	} else {
    		try {
    			ooPort = Integer.parseInt(ooPortString);
    		} catch (Exception e) {
    			resultMap.put(OutputNames.RETURN_RESULT, "-3");
                resultMap.put("resultMessage", "ooPort not readable");
                return resultMap;
    		}
    	}
    	if (ooUsername == null) ooUsername = "";
    	if (ooPassword == null) ooPassword = "";
    	
    	/*
    	 * open the channel
    	 */
    	
    	if (channelId != null && !channelId.isEmpty()) try {
    		channel = getChannel(channelId);
    	} catch (Exception e) { /* do nothing */ }
    	if (channel == null) {
    		localChannel = createChannel(mqHost, mqPortString, virtualHost, username, password);
    		channelId = localChannel.get("channelId");
    		channel = getChannel(channelId);
    	}
    	
    	/*
    	 * read the arguments
    	 */
    	if (args != null && !args.isEmpty()) try {
        	Map<String,Object> jason = new HashMap<String,Object>();
        	
    		JSONReader rdr = new JSONReader();
    		jason = (Map<String, Object>) rdr.read(args);
    		
    		for (Map.Entry<String, Object> entry: jason.entrySet()) {
    			String key = entry.getKey();
    			
    			if (entry.getValue() instanceof Integer) {
    				Integer value = (Integer) entry.getValue();
    				localArgs.put(key, value);
    			}
    			
    			if (entry.getValue() instanceof Boolean) {
    				Boolean value = (Boolean) entry.getValue();
    				localArgs.put(key, value);
    			}
    			
    			if (entry.getValue() instanceof String) {
    				String value = (String) entry.getValue();
    				localArgs.put(key, value);
    			}
    		}
        } catch (Exception e) {
        	resultMap.put(OutputNames.RETURN_RESULT, "-3");
            resultMap.put("resultMessage", "could not read arguments");
            return resultMap;
        }
    	
    	/*
    	 * create consumer must have autoAck set to false. We need to do this in
    	 * in method. Next we set noLocal to false.
    	 */
    	
    	QConsumer consumer = new QConsumer(channel) {
    	@Override
    	public void handleDelivery(String consumerTag,
    			Envelope envelope,
    			AMQP.BasicProperties properties,
    			byte[] body) throws IOException {
    		Channel channel = getChannel();
    		String ooUser = getOOUser();
    		String ooPass = getOOPass();
    		String flow = getFlow();
    		String runName = getRunName();
    		String ooHost = getOOHost();
    		Integer ooPort = getOOPort();
    		String mqHost = getMQHost();
    		String mqPort = getMQPort();
    		String mqUser = getMQUser();
    		String mqPass = getMQPass();
    		String virtualHost = getVirtualHost();
    		String queueName = getQueueName();
    		String corrId;
    		RestApi rest = new RestApi();
    		ArrayList<GetResponse> mesgList = new ArrayList<GetResponse>();
    		Map<String,Object> flowInput = new HashMap<String,Object>();
    		JSONReader rdr = new JSONReader();
    		JSONWriter wtr = new JSONWriter();
    		
    		System.out.println("handleDelivery: "+runName);
    		
    		/* 
    		 * get all mandatory input fields for a flow
    		 */
    		Map<String,Boolean> flowInputMap = 
    				rest.getFlowInputMap(ooHost, ooPort.toString(), ooUser, ooPass, flow, true);	
    	
    		/*
    		 * when the runName is not given we set it to the conumerTag
    		 */
    		if (runName.isEmpty()) runName = rest.getFlowName(ooHost, ooPort.toString(), 
    				ooUser,ooPass, flow) + ": received a message";
    		
    		/*
    		 * get the correlation id to find messages for the same flow
    		 */
    		corrId = properties.getCorrelationId();
    		if (corrId == null) corrId = "please add a correlation ID"; 
    		
    		/*
    		 * put the OO flow input fiels on a stack.
    		 * before we call the flow we will use JSONWriter to
    		 * build a json string from this stack.
    		 */
    		Map<String,Object> headers = new HashMap<String,Object>();
    		if (properties.getHeaders() != null) {
    			headers = properties.getHeaders();
    			Map<String,Object> jHeader = new HashMap<String,Object>();
        	
    			jHeader = (Map<String, Object>) rdr.read(headers.get("flowInput").toString());
    			for (Map.Entry<String,Object> entry:jHeader.entrySet()) {
    				flowInput.put(entry.getKey(), entry.getValue());
    			}
    			
    		}
    		
    		System.out.println("inputs 1: "+flowInput.toString());
    		   		
    		/*
    		 * when we do not have all input fields we try to get them
    		 * from messages already sent and still unacked in the queue
    		 */
    		int mapCount = 0;
    		for (Map.Entry<String,Boolean> item : flowInputMap.entrySet()) {
    			Object value = flowInput.get(item.getKey());
    			if (value != null) ++mapCount;
    		}
    		
    		/* 
    		 * when mapCount equals flowInput.Size then we already have all 
    		 * required input fields.
    		 */
    		if (mapCount < flowInput.size()) {
    			mesgList = getMessagesByCorrId(mqHost, mqPort, mqUser, mqPass, virtualHost, queueName, corrId);
    		
    			for (int index=0; mesgList != null && index<mesgList.size(); index++) {
    				if (mesgList.get(index).getProps().getHeaders() != null) {
    					headers = mesgList.get(index).getProps().getHeaders();
    					Map<String,Object> jHeader = new HashMap<String,Object>();
            		
    					jHeader = (Map<String, Object>) rdr.read(headers.get("flowInput").toString());
    					for (Map.Entry<String,Object> entry:jHeader.entrySet()) {
    						flowInput.put(entry.getKey(), entry.getValue());
    					}
    				}
    			}
    		}
    		System.out.println("inputs 2: "+flowInput.toString());
    		
    		/* add or override correlation ID */
    		flowInput.put("correlationId", corrId);
    		
    		/*
    		 * check that we have met all requirements to start the flow.
    		 * this means we need to check that all required inputs are provided.
    		 * so we count the inputs that equal the required once.
    		 * if the count matches the size of the flowInputMap than we have all
    		 * required fields. 
    		 */
    		mapCount = 0;
    		for (Map.Entry<String,Boolean> item : flowInputMap.entrySet()) {
    			Object value = flowInput.get(item.getKey());
    			if (value != null) ++mapCount;
    		}
    		Boolean flowInputsOkay = mapCount == flowInputMap.size();
    		
    		String inputs = wtr.write(flowInput);
    		if (flowInputsOkay) System.out.println("input: "+inputs);
    		
    		String postLink = "https://"+ooHost+":"+ooPort.toString()+"/oo/rest/executions/";
    		String postRequest = "{\"uuid\":\""+flow+"\",\"runName\":\""+runName+"\","+
    				"\"logLevel\":\"INFO\",\"inputs\":"+inputs+"}";
    		
    		System.out.println("Input: "+inputs);
    		
    		if (flowInputsOkay) try {
    			System.out.println("Posting: "+postRequest);
    			rest.httpPost(postLink, ooUser, ooPass, postRequest);
    			channel.basicAck(envelope.getDeliveryTag(), false);
    		} catch (Exception e) {
    			// System.err.println("could not start flow");
    			try {
    				channel.basicNack(envelope.getDeliveryTag(), false, false);
    				for (int index=0; index<mesgList.size(); index++) {
    					channel.basicAck(mesgList.get(index).getEnvelope().getDeliveryTag(), false);
    				}
    					
    			} catch (Exception f) { /* do nothing */ }
    		} else {
    			try {
    				channel.basicNack(envelope.getDeliveryTag(), false, true);
    				channel.basicRecover(false);
    			} catch (Exception f) { /* do nothing */ }
    		}
    	} /* end handleDelivery */
    	}; /* end QConsumer */;
		
    	consumer.setOOHost(ooHost);
    	consumer.setOOPort(ooPort);
    	consumer.setOOUser(ooUsername);
    	consumer.setOOPass(ooPassword);
    	consumer.setMQHost(mqHost);
    	consumer.setMQPort(mqPortString);
    	consumer.setMQUser(username);
    	consumer.setMQPass(password);
    	consumer.setVirtualHost(virtualHost);
    	consumer.setFlow(flowUuid);
    	consumer.setRunName(runName);
    	consumer.setQueueName(queueName);
    	
    	/* 
    	 * create the consumer HERE
    	 */
    	
    	try {
			channel.basicConsume(queueName, /* autoAck */ false, consumerTag, /* noLocal */ false, 
					getBool(exclusive, false), localArgs, consumer);
		} catch (IOException e) {
			resultMap.put(OutputNames.RETURN_RESULT, "-1");
			resultMap.put("resultMessage", "could not create consumer");
			return resultMap;
		}
            
    	
    	resultMap.put(OutputNames.RETURN_RESULT, "0");
		resultMap.put("resultMessage", "consumer created");
		resultMap.put("channelId", channelId);
		
		return resultMap;
	}
}

