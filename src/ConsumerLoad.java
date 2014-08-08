import com.csvreader.CsvReader;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.net.URI;
import java.util.*;

public class ConsumerLoad {
    public static void Run(String path,BatchInserter inserter) throws Exception {

        BatchInserterIndexProvider indexProvider = new LuceneBatchInserterIndexProvider(inserter);
        BatchInserterIndex iNames =indexProvider.nodeIndex("Names", MapUtil.stringMap("type","exact"));
        BatchInserterIndex iPin =indexProvider.nodeIndex("Pin", MapUtil.stringMap("type","exact"));
        Label personLabel = DynamicLabel.label("Consumer");
        int counter=0;
            CsvReader consumerReader = new CsvReader(path + "Neo4JConsumer.csv");
            Reader namesReader = new Reader(path + "Neo4JConsumerName.csv");
            consumerReader.readHeaders();
            Long curr=System.currentTimeMillis();
            while (consumerReader.readRecord()) {
                Map<String, Object> properties = new HashMap<String, Object>();
                final String consumerPin = consumerReader.get("ConsumerPin");
                properties.put("ConsumerPin", consumerPin);
                final  String  cId = consumerReader.get("cId");
                final String[] fullNames = GetArray(namesReader.Get(cId), "FullName");
                if(fullNames.length==0)
                    System.out.println("No names:"+consumerPin);
                else
                    properties.put("names", fullNames);
                final Long node = Long.parseLong(cId);
                inserter.createNode(node, properties, personLabel);
                for(String name : fullNames)
                    iNames.add(node,MapUtil.map("names",name));
                iPin.add(node, MapUtil.map("ConsumerPin",consumerPin));
                counter++;
                if(counter%1000000==0)
                {
                    iNames.flush();
                    iPin.flush();
                    System.out.print(counter/1000000);
                    System.out.print("M Consumers in ");
                    System.out.println(System.currentTimeMillis()-curr);
                    curr=System.currentTimeMillis();
                }
            }
            consumerReader.close();
            indexProvider.shutdown();
            }
    private static String[] GetArray(MyTable values, String key) {
        try{
        int size = values.size();
        String[] res = new String[size];
        for (int i = 0; i < size; i++)
            res[i] = values.get(i).get(key);
        return res;
        }
        catch (Exception e)
        {
            return new String[0];
        }
    }
}
