import com.csvreader.CsvReader;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.io.File;
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
    BatchInserterIndex ixpr;


    public Loader(String folder) {
        _path = folder;

    }

    public void Run() throws Exception {
        final Long start = PrintTimeStamp(0l);
        Long curr = start;
        //Map<String, String> settings = MapUtil.load(new File("/home/dkrasnikov/Downloads/batch.properties"));
        final String storeDir = "/home/dkrasnikov/data/graph.db";

        inserter = BatchInserters.inserter(storeDir);
        indexProvider = new LuceneBatchInserterIndexProvider(inserter);

        wl("Loading Addresses.");
        final Integer addressRun = AddressLoad();
        w(addressRun.toString() + " in ");
        curr = PrintTimeStamp(curr);

        wl("Loading Customers with relationships");
        final Integer consumerRun = ConsumerLoad();
        w(consumerRun.toString() + " in ");
        curr = PrintTimeStamp(curr);

        wl("Loading Property with relationships.");
        PropertyLoad();
        curr = PrintTimeStamp(curr);

        wl("Loading Transactions with relationships.");
        TransLoad();
        curr = PrintTimeStamp(curr);

        System.out.println("Initializing shutdown");
        indexProvider.shutdown();
        inserter.shutdown();
        PrintTimeStamp(curr);
        w("Total time:");
        PrintTimeStamp(start);
    }

    public Integer ConsumerLoad() throws Exception {
        Integer counter = 0;
        BatchInserterIndex ixn = indexProvider.nodeIndex("names", MapUtil.stringMap("type", "exact"));
        ixp = indexProvider.nodeIndex("pin", MapUtil.stringMap("type", "exact"));
        ixp.setCacheCapacity("pin", 5000000);
        RelationshipType situsType = DynamicRelationshipType.withName("situs");
        RelationshipType mailType = DynamicRelationshipType.withName("mailing");
        Label personLabel = DynamicLabel.label("Consumer");
        CsvReader reader = Reader.GetReader(_path + "Consumer.csv");
        while (reader.readRecord()) {
            Map<String, Object> properties = new HashMap<String, Object>();
            final String consumerPin = reader.get("pin");
            properties.put("Pin", consumerPin);
            final String[] fullNames = reader.get("names").split(",");
            if (fullNames.length > 0) properties.put("Names", fullNames);
            final long node = inserter.createNode(properties, personLabel);
            for (String name : fullNames)
                ixn.add(node, MapUtil.map("names", name));
            ixp.add(node, MapUtil.map("pin", consumerPin));
            final String maids = reader.get("maids");
            if (maids != null) {
                for (String aid : maids.split(",")) {
                    final Long child = ixa.get("aid", aid).getSingle();
                    if (child != null) inserter.createRelationship(node, child, mailType, null);
                }
            }
            final String saids = reader.get("saids");
            if (saids != null) {
                for (String aid : saids.split(",")) {
                    final Long child = ixa.get("aid", aid).getSingle();
                    if (child != null) inserter.createRelationship(node, child, situsType, null);
                }
            }
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
        ixa.setCacheCapacity("pin", 5000000);
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

    //    public Integer Consumer2AddressLoad() throws Exception {
//        Integer counter = 0;
//        RelationshipType mailType = DynamicRelationshipType.withName("mailing");
//        RelationshipType situsType = DynamicRelationshipType.withName("situs");
//        CsvReader reader = Reader.GetReader(_path + "Consumer2Address.csv");
//        Key2Key lastPin = new Key2Key();
//        while (reader.readRecord()) {
//            final String pin = reader.get("pin");
//            final String type = reader.get("type");
//            if (lastPin.OrigKey!=pin) {
//                lastPin.GraphKey = ixp.get("pin", pin).getSingle();
//                if(lastPin.GraphKey==null) continue;
//                lastPin.OrigKey = pin;
//            }
//            final String[] aids = reader.get("aids").split(",");
//            for (String aid : aids) {
//                final Long child = ixa.get("aid", aid).getSingle();
//                if(child!=null)
//                    inserter.createRelationship(lastPin.GraphKey, child, type.equals("1") ? mailType : situsType, null);
//            }
//            counter++;
//        }
//        reader.close();
//        return counter;
//    }
    private void PropertyLoad() throws Exception {
        Label label = DynamicLabel.label("Property");
        final String keyName = "pcl";
        ixpr = CreateIndex(keyName);
        RelationshipType situsType = DynamicRelationshipType.withName("situs");
        RelationshipType owner = DynamicRelationshipType.withName("owner");
        CsvReader reader = Reader.GetReader(_path + "Tax.csv");
        while (reader.readRecord()) {
            Map<String, Object> properties = new HashMap<String, Object>();
            final String pcl = reader.get("pcl");
            properties.put("Pcl", pcl);
            final Long node = inserter.createNode(properties, label);
            ixpr.add(node, MapUtil.map(keyName, pcl));
            for (String aid : GetValuesCollection(reader, "aids")) {
                final Long child = ixa.get("aid", aid).getSingle();
                if (child != null) inserter.createRelationship(node, child, situsType, null);
            }
            for (String pin : GetValuesCollection(reader, "pins")) {
                final Long child = ixp.get("pin", pin).getSingle();
                if (child != null) inserter.createRelationship(node, child, owner, null);
            }
        }
        ixpr.flush();
        reader.close();
    }

    private BatchInserterIndex CreateIndex(String keyName) {
        BatchInserterIndex ix = indexProvider.nodeIndex(keyName, MapUtil.stringMap("type", "exact"));
        ix.setCacheCapacity(keyName, 5000000);
        return ix;
    }

    private String[] GetValuesCollection(CsvReader reader, String fieldName) throws Exception {
        final String value = reader.get(fieldName);
        return value == null ? new String[0] : value.split(",");
    }

    private void TransLoad() throws Exception {
        Label label = DynamicLabel.label("Trans");
        RelationshipType buyer = DynamicRelationshipType.withName("buyer");
        RelationshipType seller = DynamicRelationshipType.withName("seller");
        RelationshipType situs = DynamicRelationshipType.withName("situs");
        RelationshipType mailing = DynamicRelationshipType.withName("mailing");
        RelationshipType belongs = DynamicRelationshipType.withName("on");

        Key2Key lastKey = new Key2Key();
        CsvReader consumerReader = Reader.GetReader(_path + "Trans.csv");

        while (consumerReader.readRecord()) {
            final String tid = consumerReader.get("batch");
            final String date = consumerReader.get("date");
            final HashMap<String, Object> relationshipProp = new HashMap<String, Object>(1);
            relationshipProp.put("Date", date);

            if (lastKey.OrigKey == null || !lastKey.OrigKey.equals(tid)) {
                Map<String, Object> properties = new HashMap<String, Object>(3);
                properties.put("Tid", tid);
                properties.put("Date", date);
                properties.put("Type", consumerReader.get("dtype"));
                lastKey.GraphKey = inserter.createNode(properties, label);
                lastKey.OrigKey = tid;

                final String pcl = consumerReader.get("pcl");
                final String said = consumerReader.get("sid");
                Long propertyId, situsAddressId;
                if (pcl != null && (propertyId = ixpr.get("pcl", pcl).getSingle()) != null) {
                    inserter.createRelationship(propertyId, lastKey.GraphKey, belongs, relationshipProp);
                } else if (said != null && (situsAddressId = ixa.get("aid", said).getSingle()) != null) {
                    inserter.createRelationship(lastKey.GraphKey, situsAddressId, situs, relationshipProp);
                }
            }
            final String pin = consumerReader.get("pin");
            final String maid = consumerReader.get("maid");
            final String ctype = consumerReader.get("ctype");
            final Long consumerId = ixp.get("pin", pin).getSingle();
            if (consumerId != null) {
                inserter.createRelationship(consumerId, lastKey.GraphKey, ctype.equals("BUYR") ? buyer : seller, relationshipProp);
                if (maid != null) {
                    final Long mailAddressId = ixa.get("aid", maid).getSingle();
                    if (mailAddressId != null) {
                        final HashMap<String, Object> mailingProp = new HashMap<String, Object>(relationshipProp);
                        mailingProp.put("TId", tid);
                        //inserter.createRelationship(lastKey.GraphKey, mailAddressId, mailing, relationshipProp);
                        inserter.createRelationship(consumerId, mailAddressId, mailing, mailingProp);
                    }
                }
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
