import com.csvreader.CsvReader;
import org.neo4j.cypher.internal.commands.CreateIndex;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws Exception {
//        Map<String, String> settings = MapUtil.load(new File("e:\\neo4j\\conf\\batch.properties"));
        BatchInserter inserter = BatchInserters.inserter("/home/dkrasnikov/D/data/graph.db");

        String path = args[0];
        ConsumerLoad.Run(path,inserter);
        AddressLoad.Run(path,inserter);
        PropertyLoad.Run(path,inserter);
        Consumer2AddressLoad.Run(path,inserter);
        Consumer2PropertyLoad.Run(path,inserter);

        Long curr=PrintTimeStamp(0l);
        System.out.println("Initializing shutdown");
        inserter.shutdown();
        PrintTimeStamp(curr);
//        // open the database
//        CouchbaseClient client = new CouchbaseClient(Arrays.asList(new URI("http://127.0.0.1:8091/pools")), "default", "");
//        BatchInserter inserter = BatchInserters.inserter("e:\\data\\graph.db");
//        Label personLabel = DynamicLabel.label("Consumer");
//        Label addressLabel = DynamicLabel.label("Address");
//        Label propertyLabel = DynamicLabel.label("Property");
//        Label transactionLabel = DynamicLabel.label("Transaction");
//        Label adcLabel = DynamicLabel.label("Adc");
//
//        client.flush();
//        try {
//
//
//
//
//            RelationshipType live = DynamicRelationshipType.withName("LIVE");
//            RelationshipType owns = DynamicRelationshipType.withName("OWN");
//            RelationshipType has = DynamicRelationshipType.withName("HAS");
//
//            CsvReader consumerReader = new CsvReader(args[0] + "PinConsumer.csv");
//            Reader namesReader = new Reader(args[0] + "PinName.csv");
//            Reader addressReader = new Reader(args[0] + "PinAddress.csv");
//            Reader propertyReader = new Reader(args[0] + "PinProperty.csv");
//            Reader tranReader = new Reader(args[0] + "PinTran.csv");
//            Reader adcReader = new Reader(args[0]+"PinAdc.csv");
//
//
//            consumerReader.readHeaders();
//            String[] headers = consumerReader.getHeaders();
//            int counter = 0;
//            while (consumerReader.readRecord()) {
//                Map<String, Object> properties = new HashMap<String, Object>();
//                String consumerId = consumerReader.get("CONSUMER_PIN");
//                properties.put("CONSUMER_PIN", consumerId);
//                properties.put("names", GetArray(namesReader.Get(consumerId), "FULL_NM"));
//                Long node = inserter.createNode(properties, personLabel);
//
//                MyTable addresses = addressReader.Get(consumerId);
//                if (addresses != null)
//                    SubmitChildren(node, inserter, client, addresses, "A", addressLabel, live);
//
//                MyTable props = propertyReader.Get(consumerId);
//                if (props != null)
//                    SubmitChildren(node, inserter, client, props, "P", propertyLabel, owns);
//
//                MyTable trans = tranReader.Get(consumerId);
//                if (trans != null)
//                    SubmitChildren(node, inserter, client, trans, "T", transactionLabel, has);
//
//                MyTable adcs = adcReader.Get(consumerId);
//                if (adcs != null)
//                    SubmitChildren(node, inserter, client, adcs, "D", adcLabel, has);
//                counter++;
//                if(counter%100000==0)
//                    System.out.print(counter);
//            }
//            System.out.print(counter);
//            consumerReader.close();
//        } finally {
//            IndexCreator nameIndex=inserter.createDeferredSchemaIndex(personLabel);
//            nameIndex.on("names");
//            nameIndex.create();
//            inserter.createDeferredSchemaIndex(personLabel).on("ConsumerId");
//            inserter.shutdown();
//            client.shutdown();
//        }
    }

//    private static void SubmitChildren(Long node, BatchInserter inserter, CouchbaseClient client, MyTable records,
//                                       String prefix, Label label, RelationshipType rType) {
//        for (LinkedHashMap<String, String> record : records) {
//            String key = GetKey(record, prefix);
//            Object value = client.get(key);
//            Long a;
//            if (value != null) {
//                a = (Long) value;
//            } else {
//                record.put("Type",label.name());
//                a = inserter.createNode(new HashMap<String, Object>(record), label);
//                client.set(key, a);
//            }
//            inserter.createRelationship(node, a, rType, null);
//        }
//    }
//
//    private static String[] GetArray(MyTable values, String key) {
//        int size = values.size();
//        String[] res = new String[size];
//        for (int i = 0; i < size; i++)
//            res[i] = values.get(i).get(key);
//        return res;
//    }
//
//    private static String GetKey(LinkedHashMap<String, String> record, String prefix) {
//        prefix = prefix.concat("_");
//        for (String key : record.keySet())
//            prefix = prefix.concat(record.get(key));
//        return prefix;
//    }
public static Long PrintTimeStamp(Long curr) {
    System.out.println(System.currentTimeMillis()-curr);
    curr=System.currentTimeMillis();
    return curr;
}
}