import com.hp.oo.sdk.content.annotations.Action;
import com.hp.oo.sdk.content.annotations.Output;
import com.hp.oo.sdk.content.annotations.Param;
import com.hp.oo.sdk.content.annotations.Response;
import com.hp.oo.sdk.content.constants.OutputNames;
import com.hp.oo.sdk.content.constants.ResponseNames;
import com.hp.oo.sdk.content.plugin.ActionMetadata.MatchType;
import com.hp.oo.sdk.content.plugin.ActionMetadata.ResponseType;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.tools.json.JSONReader;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
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
	
	@SuppressWarnings("unchecked")
	@Action(name = "send message",
            description = "sends a message with RabbitMQ\n" +
            		"\nInputs:\n" +
            		"message: the message to send\n" +
            		"mqHost: FQDN or ip address of the rabbitMQ host\n" +
            		"mqPort: port number of the rabbitMQ host" +
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
            		"\nresultMessage: indicates success or failure reason.",
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
    		jason = (Map<String, Object>) rdr.read(headers);
    		
    		for (Map.Entry<String, Object> entry: jason.entrySet()) {
    			String key = entry.getKey();
    			
    			if (entry.getValue().getClass().toString().equals("class java.lang.Integer")) {
    				Integer value = (Integer) entry.getValue();
    				localHeaders.put(key, value);
    			}
    			
    			if (entry.getValue().getClass().toString().equals("class java.lang.Boolean")) {
    				Boolean value = (Boolean) entry.getValue();
    				localHeaders.put(key, value);
    			}
    			
    			if (entry.getValue().getClass().toString().equals("class java.lang.String")) {
    				String value = (String) entry.getValue();
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
            		"mqHost: rabbitMQ hostname or ip address\n" +
            		"mqPort: port of the rabbitMQ host, defaults to 5672\n" +
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
        /*  } catch (NullPointerException e) {
        	String rMesg = "message not retrieved: null pointer: "+err.toString()+"\n"+e.getStackTrace().toString();
        	resultMap.put(OutputNames.RETURN_RESULT, "-3");
        	resultMap.put("resultMessage", rMesg);
        	resultMap.put("channelId", channelId);
        	return resultMap;
        } catch (Exception e) {
        	resultMap.put(OutputNames.RETURN_RESULT, "-2");
        	resultMap.put("resultMessage", "an error occured while reading message");
        	resultMap.put("channelId", channelId);
        	return resultMap;
        } */
        
        /*
         * we need to create a JSON string for the headers
         */
        Map<String,Object> headers = props.getHeaders();
        String outHeaders = "";
        if (headers != null) {
        	int multipleEntries = 0;
        	for (Map.Entry<String, Object> entry: headers.entrySet()) {
        		
        		String type = entry.getValue().getClass().toString();
        		
        		if (type.endsWith("Integer")) {
        			if (multipleEntries > 0) outHeaders += ","; else outHeaders = "{";
        			++multipleEntries;
        			outHeaders += "\""+entry.getKey()+"\":"+entry.getValue().toString();
        		}
        		
        		if (type.endsWith("Boolean")) {
        			if (multipleEntries > 0) outHeaders += ","; else outHeaders = "{";
        			++multipleEntries;
        			outHeaders += "\""+entry.getKey()+"\":"+entry.getValue().toString();
        		}
        		
        		if (type.endsWith("String")) {
        			if (multipleEntries > 0) outHeaders += ","; else outHeaders = "{";
        			++multipleEntries;
        			outHeaders += "\""+entry.getKey()+"\":\""+entry.getValue().toString()+"\"";
        		}
        		
        	}
        	outHeaders += "}";
        }
        
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
        resultMap.put("headers", outHeaders);
        resultMap.put("messageId", props.getMessageId());
        if (props.getPriority() != null) resultMap.put("priority", props.getPriority().toString());
        resultMap.put("replyTo", props.getReplyTo());
		resultMap.put("timestamp", time);
		resultMap.put("type", props.getType());
		resultMap.put("userId", props.getUserId()); 
        
        return resultMap;
	}
	
	@Action(name = "acknowledge message",
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
        try {
        	Channel channel = null;
        	
        	if (channelId != null && !channelId.isEmpty()) channel = getChannel(channelId);
        	
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
        } catch (NullPointerException e) {
        	resultMap.put(OutputNames.RETURN_RESULT, "-1");
        	resultMap.put("resultMessage", "could not ack: null pointer");
        	return resultMap;
        } catch (Exception e) {
        	resultMap.put("resultMessage", "an error occured: "+err.toString());
        	resultMap.put(OutputNames.RETURN_RESULT, "-1");
        	return resultMap;
        }
		
        resultMap.put("resultMessage", "message(s) ack'ed");
    	resultMap.put(OutputNames.RETURN_RESULT, "0");
        return resultMap;
	}
	
	@Action(name = "reject acknowledge message",
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
        try {
        	Channel channel = null;
        	
        	if (channelId != null && !channelId.isEmpty()) channel = getChannel(channelId);
        	
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
        } catch (NullPointerException e) {
        	resultMap.put(OutputNames.RETURN_RESULT, "-1");
        	resultMap.put("resultMessage", "could not nack: null pointer");
        	return resultMap;
        } catch (Exception e) {
        	resultMap.put("resultMessage", "an error occured");
        	resultMap.put(OutputNames.RETURN_RESULT, "-1");
        	return resultMap;
        }
		
        resultMap.put("resultMessage", "nack'ed message");
        resultMap.put(OutputNames.RETURN_RESULT, "0");
		return resultMap;
	}
	
	@Action(name = "recover channel",
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
	        try {
	        	Channel channel = null;
	        	
	        	if (channelId != null && !channelId.isEmpty()) channel = getChannel(channelId);
	        	
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
	        } catch (NullPointerException e) {
	        	resultMap.put(OutputNames.RETURN_RESULT, "-1");
	        	resultMap.put("resultMessage", "could not recover: null pointer");
	        	return resultMap;
	        } catch (Exception e) {
	        	resultMap.put("resultMessage", "an error occured");
	        	resultMap.put(OutputNames.RETURN_RESULT, "-1");
	        	return resultMap;
	        }
			
	        resultMap.put("resultMessage", "channel recovered");
	        resultMap.put(OutputNames.RETURN_RESULT, "0");
			return resultMap;
		}
	
	@Action(name = "create channel",
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
}
