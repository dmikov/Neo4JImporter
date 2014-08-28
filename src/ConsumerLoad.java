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
    public static void Run(String path, BatchInserter inserter) throws Exception {

        BatchInserterIndexProvider indexProvider = new LuceneBatchInserterIndexProvider(inserter);
        BatchInserterIndex iNames =indexProvider.nodeIndex("names", MapUtil.stringMap("type","exact"));
        BatchInserterIndex iPin =indexProvider.nodeIndex("pin", MapUtil.stringMap("type","exact"));
        Label personLabel = DynamicLabel.label("Consumer");
        int counter=0;
        CsvReader consumerReader = new CsvReader(path + "Consumer.csv");
        consumerReader.readHeaders();
        Long curr=System.currentTimeMillis();
        while (consumerReader.readRecord()) {
            Map<String, Object> properties = new HashMap<String, Object>();
            final String consumerPin = consumerReader.get("pin");
            properties.put("Pin", consumerPin);
            final String[] fullNames = consumerReader.get("names").split(",");
            if(fullNames.length==0)
                System.out.println("No names:"+consumerPin);
            else
                properties.put("Names", fullNames);
            final long node = inserter.createNode(properties, personLabel);
            for(String name : fullNames)
                iNames.add(node,MapUtil.map("names",name));
            iPin.add(node, MapUtil.map("pin",consumerPin));
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
 }
