package cs505finaltemplate.graphDB;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
public class GraphDBEngine {


	public static OrientDB orient = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
    public static ODatabaseSession db = orient.open("test", "root", "rootpwd");

    //!!! CODE HERE IS FOR EXAMPLE ONLY, YOU MUST CHECK AND MODIFY!!!
    public GraphDBEngine() {}

    public void startDB(){
	clearDB(db);

        //create classes
       OClass patient = db.getClass("patient");
 
        if (patient == null) {
            patient = db.createVertexClass("patient");
        }

        if (patient.getProperty("patient_mrn") == null) {
            //patient.createProperty("patient_mrn", OType.STRING).setMandatory(true).setNotNull(true);
            //patient.createIndex("patient_mrn_index", OClass.INDEX_TYPE.UNIQUE, "patient_mrn");
	    patient.createProperty("patient_mrn", OType.STRING);
	    patient.createIndex("patient_mrn_index", OClass.INDEX_TYPE.NOTUNIQUE, "patient_mrn");
        }

        if (patient.getProperty("hospital_status") == null) {
            patient.createProperty("hospital_status", OType.INTEGER);
        }

        if (patient.getProperty("vax_status") == null) {
            patient.createProperty("vax_status", OType.INTEGER);
        }
        if (patient.getProperty("testing_id") == null) {
            patient.createProperty("testing_id", OType.INTEGER);
        }
        if (patient.getProperty("patient_name") == null) {
            patient.createProperty("patient_name", OType.STRING);
        }
        if (patient.getProperty("patient_zipcode") == null) {
            patient.createProperty("patient_zipcode", OType.INTEGER);
        }
        if (patient.getProperty("patient_status") == null) {
            patient.createProperty("patient_status", OType.INTEGER);
        }
        if (patient.getProperty("contact_list") == null) {
            patient.createProperty("contact_list", OType.EMBEDDEDLIST);
        }
        if (patient.getProperty("event_list") == null) {
            patient.createProperty("event_list", OType.EMBEDDEDLIST);
        }

        if (db.getClass("contact_with") == null) {
                db.createEdgeClass("contact_with");
            }
        if (db.getClass("event_with") == null) {
            db.createEdgeClass("event_with");
        }


        OClass hospital = db.getClass("hospital");
 
        if (hospital == null) {
            hospital = db.createVertexClass("hospital");
        }
        if (hospital.getProperty("hospital_id") == null) {
            hospital.createProperty("hospital_id", OType.INTEGER);
        }
        if (hospital.getProperty("patient_name") == null) {
            hospital.createProperty("patient_name", OType.STRING);
        }
        if (hospital.getProperty("patient_mrn") == null) {
            hospital.createProperty("patient_mrn", OType.STRING);
        }
        if (hospital.getProperty("patient_status") == null) {
            hospital.createProperty("patient_status", OType.INTEGER);
        }

        OClass vax = db.getClass("vax");
 
        if (vax == null) {
            vax = db.createVertexClass("vax");
        }
        if (vax.getProperty("vaccination_id") == null) {
            vax.createProperty("vaccination_id", OType.INTEGER);
        }
        if (vax.getProperty("patient_name") == null) {
            vax.createProperty("patient_name", OType.STRING);
        }
        if (vax.getProperty("patient_mrn") == null) {
            vax.createProperty("patient_mrn", OType.STRING);
        }
    }

    public void endDB(){
    	db.close();
        orient.close();
    }

    //public OVertex createPatient(ODatabaseSession db, String patient_mrn, int hospital_status, int vax_status) {
    public OVertex createPatient(String patient_mrn, int hospital_status, int vax_status, int testing_id, String patient_name,int patient_zipcode,int patient_status, List<String> contact_list, List<String> event_list) {    
    	OVertex result = db.newVertex("patient");
        result.setProperty("patient_mrn", patient_mrn);
	    result.setProperty("hospital_status", hospital_status);
	    result.setProperty("vax_status", vax_status);
        result.setProperty("testing_id", testing_id);
        result.setProperty("patient_name", patient_name);
        result.setProperty("patient_zipcode", patient_zipcode);
        result.setProperty("patient_status", patient_status);
        result.setProperty("contact_list", contact_list);
        result.setProperty("event_list", event_list);
        for(String contact : contact_list){
            String query = "select from patient where patient_mrn = ?";
            OResultSet rs = db.query(query, contact);
            while (rs.hasNext()) {
                rs.next().getVertex().ifPresent(x->{
                  result.addEdge(x, "contact_with");
                  });
            }
            rs.close();
        }
        for(String event : event_list){
            String query = "select from patient where event_list contains ?";
            OResultSet rs = db.query(query, event);
            while (rs.hasNext()) {
                rs.next().getVertex().ifPresent(x->{
                  result.addEdge(x, "event_with");
                  });
            }
            rs.close();
        }
        result.save();
        return result;
    }

    public OVertex createHospital(int hospital_id, String patient_name, String patient_mrn, int patient_status){
        OVertex result = db.newVertex("hospital");
        result.setProperty("hospital_id", hospital_id);
        result.setProperty("patient_name", patient_name);
        result.setProperty("patient_mrn", patient_mrn);
        result.setProperty("patient_status", patient_status);
        result.save();
        return result;
    }

    public OVertex createVax(int vaccination_id, String patient_name, String patient_mrn){
        OVertex result = db.newVertex("vax");
        result.setProperty("vaccination_id", vaccination_id);
        result.setProperty("patient_name", patient_name);
        result.setProperty("patient_mrn", patient_mrn);
        result.save();
        return result;
    } 

    
    private void clearDB(ODatabaseSession db) {

        String query = "DELETE VERTEX FROM patient";
        db.command(query);

    }
    
    public List<String> getcontactlist(String mrn){
        String query = "TRAVERSE inE('contact_with'), outE('contact_with'), inV(), outV() FROM (select from patient where patient_mrn = ?) WHILE $depth <= 2";
        OResultSet rs = db.query(query, mrn);
        List<String> contactlist = new ArrayList<String>();
        while (rs.hasNext()) {
            OResult item = rs.next();
            if(!item.isEdge()){
            contactlist.add(item.getProperty("patient_mrn").toString());
            }
        }

        rs.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
        return contactlist;
    } 
    public Map<String,List<String>> getpossiblecontactlist(String mrn){
        String query = "select event_list from patient where patient_mrn = ?";
        Map<String,List<String>> contactlist = new HashMap<>();
        OResultSet rs = db.query(query, mrn);
        while (rs.hasNext()) {
            OResult item = rs.next();
            String eventstring = item.getProperty("event_list").toString().replaceAll("\\s+", "").replace("[", "").replace("]","");
            String[] events = eventstring.split(",");
            contactlist = loopevents(events);
        }

        rs.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
        System.out.println("final:" + contactlist);
        return contactlist;
    } 
    
    private List<String> geteventwith(String event){
        String query = "TRAVERSE inE('event_with'), outE('event_with'), inV(), outV() FROM (select from patient where event_list contains ?) WHILE $depth <= 2";
        List<String> patient_list = new ArrayList<String>();
        OResultSet rs = db.query(query,event);
        while (rs.hasNext()) {
            OResult item = rs.next();
            if(!item.isEdge()){
            patient_list.add(item.getProperty("patient_mrn").toString());
            }
        }
        rs.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
        return patient_list;
    } 
    
    private Map<String,List<String>> loopevents(String[] events){
        Map<String,List<String>> contactlist = new HashMap<>();
        int i=0;
            while(i < events.length ){
                List<String> patient_list = new ArrayList<String>();
                patient_list = geteventwith(events[i]);
                if(!patient_list.isEmpty()){ 
                    contactlist.put(events[i],patient_list);
                }
                i++;
            }
            return contactlist;
    }
}
