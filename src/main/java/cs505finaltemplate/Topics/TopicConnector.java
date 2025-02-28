package cs505finaltemplate.Topics;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import cs505finaltemplate.Launcher;
import io.siddhi.query.api.expression.condition.In;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TopicConnector {

    private Gson gson;

    final Type typeOfListMap = new TypeToken<List<Map<String,String>>>(){}.getType();
    final Type typeListTestingData = new TypeToken<List<TestingData>>(){}.getType();

    //private String EXCHANGE_NAME = "patient_data";
    Map<String,String> config;

    public TopicConnector(Map<String,String> config) {
        gson = new Gson();
        this.config = config;
    }

    public void connect() {

        try {

            //create connection factory, this can be used to create many connections
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(config.get("hostname"));
            factory.setPort(Integer.parseInt(config.get("port")));
            factory.setUsername(config.get("username"));
            factory.setPassword(config.get("password"));
            factory.setVirtualHost(config.get("virtualhost"));

            //create a connection, many channels can be created from a single connection
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            patientListChannel(channel);
            hospitalListChannel(channel);
            vaxListChannel(channel);

        } catch (Exception ex) {
            System.out.println("connect Error: " + ex.getMessage());
            ex.printStackTrace();
        }
}

    private void patientListChannel(Channel channel) {

        
    //"Example JSON:

    //[{

    //  "testing_id": 001,

    //  "patient_name": "John Prine",

    //  "patient_mrn": "024c60d2-a1eb",

    //  "patient_zipcode": 40351,
    //  "patient_status": 1,

     

    //  "contact_list": ["498d-8739", "0d2-a1eb-498"],
    //  "event_list": ["234fs-3493", "fsf545-dfs54"]

    //}]

        try {
	    
	        System.out.println("Creating patient_list channel");

            String topicName = "patient_list";

            channel.exchangeDeclare(topicName, "topic");
            String queueName = channel.queueDeclare().getQueue();

            channel.queueBind(queueName, topicName, "#");


            System.out.println(" [*] Paitent List Waiting for messages. To exit press CTRL+C");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {

            String message = new String(delivery.getBody(), "UTF-8");

		    Launcher.graphDBEngine.db.activateOnCurrentThread();

            List<TestingData> incomingList = gson.fromJson(message, typeListTestingData);

            for (TestingData testingData : incomingList) {	
		        OVertex patient = Launcher.graphDBEngine.createPatient(testingData.patient_mrn, -1, -1,testingData.testing_id,testingData.patient_name,testingData.patient_zipcode,testingData.patient_status,testingData.contact_list,testingData.event_list);
                //for(String contact : testingData.contact_list){
                //OVertex c = Launcher.graphDBEngine.createPatient(contact, -1, -1);
                
                //OEdge cEdge = patient.addEdge(c, "contact_with");
                //cEdge.save();
                //}

		    if(testingData.patient_status == 1) {
			//Data to send to CEP
			    Map<String,String> zip_entry = new HashMap<>();
                zip_entry.put("zip_code",String.valueOf(testingData.patient_zipcode));
                String testInput = gson.toJson(zip_entry);
                //uncomment for debug
                //System.out.println("testInput: " + testInput);

                //insert into CEP
                Launcher.cepEngine.input("testInStream",testInput);

                //do something else with each record
                /*
                System.out.println("*Java Class*");
                System.out.println("\ttesting_id = " + testingData.testing_id);
                System.out.println("\tpatient_name = " + testingData.patient_name);
                System.out.println("\tpatient_mrn = " + testingData.patient_mrn);
                System.out.println("\tpatient_zipcode = " + testingData.patient_zipcode);
                System.out.println("\tpatient_status = " + testingData.patient_status);
                System.out.println("\tcontact_list = " + testingData.contact_list);
                System.out.println("\tevent_list = " + testingData.event_list);
                    */
		    }
                }

            };

            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
            });

        } catch (Exception ex) {
            System.out.println("patientListChannel Error: " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    private void hospitalListChannel(Channel channel) {
        try {

            String topicName = "hospital_list";

            System.out.println("Creating hospital_list channel");

            channel.exchangeDeclare(topicName, "topic");
            String queueName = channel.queueDeclare().getQueue();

            channel.queueBind(queueName, topicName, "#");

            System.out.println(" [*] Hospital List Waiting for messages. To exit press CTRL+C");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {

                //new message
                String message = new String(delivery.getBody(), "UTF-8");

                //convert string to class
                List<Map<String,String>> incomingList = gson.fromJson(message, typeOfListMap);
                Launcher.graphDBEngine.db.activateOnCurrentThread();
                for (Map<String,String> hospitalData : incomingList) {
                    int hospital_id = Integer.parseInt(hospitalData.get("hospital_id"));
                    String patient_name = hospitalData.get("patient_name");
                    String patient_mrn = hospitalData.get("patient_mrn");
                    int patient_status = Integer.parseInt(hospitalData.get("patient_status"));
                    OVertex hospital = Launcher.graphDBEngine.createHospital(hospital_id, patient_name, patient_mrn, patient_status);
                    //do something with each each record.
                }

            };

            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
            });

        } catch (Exception ex) {
            System.out.println("hospitalListChannel Error: " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    private void vaxListChannel(Channel channel) {
        try {

            String topicName = "vax_list";

            System.out.println("Creating vax_list channel");

            channel.exchangeDeclare(topicName, "topic");
            String queueName = channel.queueDeclare().getQueue();

            channel.queueBind(queueName, topicName, "#");


            System.out.println(" [*] Vax List Waiting for messages. To exit press CTRL+C");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {

                String message = new String(delivery.getBody(), "UTF-8");

                //convert string to class
                Launcher.graphDBEngine.db.activateOnCurrentThread();
                List<Map<String,String>> incomingList = gson.fromJson(message, typeOfListMap);
                for (Map<String,String> vaxData : incomingList) {
                    int vaccination_id = Integer.parseInt(vaxData.get("vaccination_id"));
                    String patient_name = vaxData.get("patient_name");
                    String patient_mrn = vaxData.get("patient_mrn");
                    OVertex vax = Launcher.graphDBEngine.createVax(vaccination_id, patient_name, patient_mrn);
                    //do something with each each record.
                }

            };

            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
            });

        } catch (Exception ex) {
            System.out.println("vaxListChannel Error: " + ex.getMessage());
            ex.printStackTrace();
        }
    }

}
