package cs505finaltemplate.CEP;

import cs505finaltemplate.Launcher;
import io.siddhi.core.util.transport.InMemoryBroker;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

public class OutputSubscriber implements InMemoryBroker.Subscriber {

    private String topic;

    public OutputSubscriber(String topic, String streamName) {
        this.topic = topic;
    }

    @Override
    public void onMessage(Object msg) {

        try {
            System.out.println("OUTPUT CEP EVENT: " + msg);
            System.out.println("");

            //You will need to parse output and do other logic,
            //but this sticks the last output value in main
            Launcher.lastCEPOutput = String.valueOf(msg);

            //String[] sstr = String.valueOf(msg).split(":");
            //String[] outval = sstr[2].split("}");
            //Launcher.accessCount = Long.parseLong(outval[0]);
	    
	    String[] sstr = String.valueOf(msg).split("\\D+");
	    Map<String, Integer> zipCount = new HashMap<String, Integer>();
	    for (int i = 1; i < sstr.length; i+=2){
		zipCount.put(sstr[i], Integer.parseInt(sstr[i+1]));
	    }

	

	    List<Integer> zipList = new ArrayList<>();

	    for (String key : zipCount.keySet()) {
		    if (Launcher.zipCount.containsKey(key)){
			if(zipCount.get(key) >= 2*Launcher.zipCount.get(key)){
				zipList.add(Integer.parseInt(key));
			}
		    }
	    }

	    Launcher.zipCount = zipCount;
	    Launcher.zipList = zipList;
	    Launcher.stateStatus = (zipList.size() >= 5) ? 1 : 0;

        } catch(Exception ex) {
            ex.printStackTrace();
        }

    }

    @Override
    public String getTopic() {
        return topic;
    }

}
