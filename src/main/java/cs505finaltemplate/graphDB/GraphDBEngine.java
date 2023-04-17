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

	if (db.getClass("contact_with") == null) {
            db.createEdgeClass("contact_with");
        }
	/*
	OVertex patient_0 = createPatient("mrn_0", -1, -1);
        OVertex patient_1 = createPatient("mrn_1", -1, -1);
        OVertex patient_2 = createPatient("mrn_2", -1, -1);
        OVertex patient_3 = createPatient("mrn_3", -1, -1);

        //patient 0 in contact with patient 1
        OEdge edge1 = patient_0.addEdge(patient_1, "contact_with");
        edge1.save();
        //patient 2 in contact with patient 0
        OEdge edge2 = patient_2.addEdge(patient_0, "contact_with");
        edge2.save();

        //you should not see patient_3 when trying to find contacts of patient 0
        OEdge edge3 = patient_3.addEdge(patient_2, "contact_with");
        edge3.save();

        getContacts(db, "mrn_0");
	*/
    }

    public void endDB(){
    	db.close();
        orient.close();
    }

    //public OVertex createPatient(ODatabaseSession db, String patient_mrn, int hospital_status, int vax_status) {
    public OVertex createPatient(String patient_mrn, int hospital_status, int vax_status) {    
    	OVertex result = db.newVertex("patient");
        result.setProperty("patient_mrn", patient_mrn);
	result.setProperty("hospital_status", hospital_status);
	result.setProperty("vax_status", vax_status);
        result.save();
        return result;
    }

    private void getContacts(ODatabaseSession db, String patient_mrn) {

        String query = "TRAVERSE inE(), outE(), inV(), outV() " +
                "FROM (select from patient where patient_mrn = ?) " +
                "WHILE $depth <= 2";
        OResultSet rs = db.query(query, patient_mrn);

        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println("contact: " + item.getProperty("patient_mrn"));
        }

        rs.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
    }

    private void clearDB(ODatabaseSession db) {

        String query = "DELETE VERTEX FROM patient";
        db.command(query);

    }

}
