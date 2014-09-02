import com.csvreader.CsvReader;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by dkrasnikov on 9/2/14.
 */
public class Loader {

    private final String _path;
    BatchInserter inserter;
    LuceneBatchInserterIndexProvider indexProvider;
    BatchInserterIndex ixa;
    BatchInserterIndex ixp;


    public Loader(String folder){
    _path=folder;

}
    public void Run() throws Exception{
        Long curr = PrintTimeStamp(0l);
        //        Map<String, String> settings = MapUtil.load(new File("e:\\neo4j\\conf\\batch.properties"));
        final String storeDir = "/home/dkrasnikov/data/graph.db";

        inserter = BatchInserters.inserter(storeDir);
        indexProvider = new LuceneBatchInserterIndexProvider(inserter);

        wl("Loading Customers");
        final Integer consumerRun = ConsumerLoad();
        w(consumerRun.toString() + " in ");
        curr = PrintTimeStamp(curr);

        wl("Loading Addresses.");
        final Integer addressRun = AddressLoad();
        w(addressRun.toString() + " in ");
        curr = PrintTimeStamp(curr);

        wl("Loading Consumer to Address relationships.");
        final Integer c2aRun = Consumer2AddressLoad();
        w(c2aRun.toString() + " in ");
        curr = PrintTimeStamp(curr);

        PropertyLoad();
//        Consumer2PropertyLoad.Run(path,inserter);


        System.out.println("Initializing shutdown");
        indexProvider.shutdown();
        inserter.shutdown();
        PrintTimeStamp(curr);
    }
    public Integer ConsumerLoad() throws Exception {
        Integer counter = 0;
        BatchInserterIndex ixn = indexProvider.nodeIndex("names", MapUtil.stringMap("type", "exact"));
        ixp = indexProvider.nodeIndex("pin", MapUtil.stringMap("type", "exact"));
        Label personLabel = DynamicLabel.label("Consumer");
        CsvReader reader = Reader.GetReader(_path + "Consumer.csv");
        while (reader.readRecord()) {
            Map<String, Object> properties = new HashMap<String, Object>();
            final String consumerPin = reader.get("pin");
            properties.put("Pin", consumerPin);
            final String[] fullNames = reader.get("names").split(",");
            if (fullNames.length == 0)
                System.out.println("No names:" + consumerPin);
            else
                properties.put("Names", fullNames);
            final long node = inserter.createNode(properties, personLabel);
            for (String name : fullNames)
                ixn.add(node, MapUtil.map("names", name));
            ixp.add(node, MapUtil.map("pin", consumerPin));
            counter++;
        }
        ixn.flush();
        ixp.flush();
        reader.close();
        return counter;
    }
    public Integer AddressLoad() throws Exception {
        Integer counter = 0;
        Label label = DynamicLabel.label("Address");
        ixa = indexProvider.nodeIndex("aid", MapUtil.stringMap("type", "exact"));
        CsvReader reader = Reader.GetReader(_path + "Address.csv");
        while (reader.readRecord()) {
            Map<String, Object> properties = new HashMap<String, Object>();
            properties.put("Street", reader.get("Street"));
            properties.put("City", reader.get("City"));
            properties.put("State", reader.get("State"));
            properties.put("Zip", reader.get("Zip"));
            final String id = reader.get("Id");
            properties.put("aId", id);
            long node = inserter.createNode(properties, label);
            ixa.add(node, MapUtil.map("aid", id));
            counter++;
        }
        ixa.flush();
        reader.close();
        return counter;
    }
    public Integer Consumer2AddressLoad() throws Exception {
        Integer counter = 0;
        RelationshipType mailType = DynamicRelationshipType.withName("mailing");
        RelationshipType situsType = DynamicRelationshipType.withName("situs");
        CsvReader reader = Reader.GetReader(_path + "Consumer2Address.csv");
        Key2Key lastPin = new Key2Key();
        while (reader.readRecord()) {
            final String pin = reader.get("pin");
            final String type = reader.get("type");
            if (!lastPin.OrigKey.equals(pin)) {
                lastPin.GraphKey = ixp.get("pin", pin).getSingle();
                if(lastPin.GraphKey==null) continue;
                lastPin.OrigKey = pin;
            }
            final String[] aids = reader.get("aids").split(",");
            for (String aid : aids) {
                final Long child = ixa.get("aid", aid).getSingle();
                if(child!=null)
                    inserter.createRelationship(lastPin.GraphKey, child, type.equals("1") ? mailType : situsType, null);
            }
            counter++;
        }
        reader.close();
        return counter;
    }
    private void PropertyLoad() throws Exception {
        Label label = DynamicLabel.label("Property");
        RelationshipType situsType = DynamicRelationshipType.withName("situs");
        RelationshipType owner = DynamicRelationshipType.withName("owner");
        CsvReader consumerReader = Reader.GetReader(_path + "Tax.csv");
        while (consumerReader.readRecord()) {
            Map<String, Object> properties = new HashMap<String, Object>();
            properties.put("Pcl", consumerReader.get("pcl"));
            final Long node = inserter.createNode(properties, label);
            for (String aid : consumerReader.get("aids").split(",")){
                final Long child = ixa.get("aid", aid).getSingle();
                if(child!=null) inserter.createRelationship(node,child,situsType,null);
            }
            for (String pin : consumerReader.get("pins").split(",")){
                final Long child = ixp.get("pin", pin).getSingle();
                if(child!=null) inserter.createRelationship(node,child,owner,null);
            }
        }
        consumerReader.close();
    }

    private static Long PrintTimeStamp(Long curr) {
        System.out.println(System.currentTimeMillis() - curr);
        curr = System.currentTimeMillis();
        return curr;
    }

    private static void w(String value) {
        System.out.print(value);
    }

    private static void wl(String value) {
        System.out.println(value);
    }
}
