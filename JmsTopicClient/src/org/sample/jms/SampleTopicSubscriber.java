/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
 
package org.sample.jms;

import org.wso2.andes.jndi.discovery.DiscoveryValues;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Properties;
 
public class SampleTopicSubscriber {

 public static final String QPID_ICF = "org.wso2.andes.jndi.discovery.DynamicDiscoveryContext";
    private static final String CF_NAME = "qpidConnectionfactory";
    private static String CARBON_CLIENT_ID = "carbon";
    private static String CARBON_VIRTUAL_HOST_NAME = "carbon";
    private static String INITIAL_ADDRESSES = "10.100.4.165:9444,10.100.4.555:9234,10.100.4.165:9443";
    private static String RETRIES = "0";
    private static String CONNECTION_DELAY = "0";
    private static String CYCLE_COUNT = "2";
    private static String TRUSTSTORE_LOCATION = "/home/wso2/Documents/works/wso2mb-3.1.0/repository/resources/security/wso2carbon.jks";
    private final float TIME = 60;
    private String USER_NAME = "sidath";
    private String PASSWORD = "sidath";
    private String TOPIC_NAME = "MYTopic";
    private TopicConnection topicConnection;
    private TopicSession topicSession;

    public TopicSubscriber subscribe() throws NamingException, JMSException {
           Properties properties = new Properties();
        properties.put(Context.INITIAL_CONTEXT_FACTORY, QPID_ICF);
        properties.put(DiscoveryValues.AUTHENTICATION_CLIENT, USER_NAME +","+ PASSWORD);
        properties.put(DiscoveryValues.CF_NAME_PREFIX + DiscoveryValues.CF_NAME, INITIAL_ADDRESSES);
        properties.put(DiscoveryValues.CARBON_PROPERTIES, "" + CARBON_CLIENT_ID + "," + CARBON_VIRTUAL_HOST_NAME + "");
        properties.put(DiscoveryValues.FAILOVER_PROPERTIES, "RETRIES=" + RETRIES + ",CONNECTION_DELAY=" +
                CONNECTION_DELAY + ",CYCLE_COUNT=" + CYCLE_COUNT + "");
        properties.put(DiscoveryValues.TRUSTSTORE, TRUSTSTORE_LOCATION);
        

	InitialContext ctx = new InitialContext(properties);
        // Lookup connection factory
        TopicConnectionFactory connFactory = (TopicConnectionFactory) ctx.lookup(CF_NAME);
        topicConnection = connFactory.createTopicConnection();
        topicConnection.start();
        topicSession =
                topicConnection.createTopicSession(false, TopicSession.AUTO_ACKNOWLEDGE);
        // Send message
        System.out.println(topicConnection);
        Topic topic = topicSession.createTopic(TOPIC_NAME);
        TopicSubscriber topicSubscriber = topicSession.createSubscriber(topic);
        return topicSubscriber;
    }

    public void receive(TopicSubscriber topicSubscriber) throws NamingException, JMSException {
        Message message = topicSubscriber.receive();
        if (message instanceof TextMessage) {
            TextMessage textMessage = (TextMessage) message;
            System.out.println("Got message from topic subscriber = " + textMessage.getText());
        }

        // Housekeeping
        topicSubscriber.close();
        topicSession.close();
        topicConnection.stop();
        topicConnection.close();
    }

    
}
