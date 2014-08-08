import com.csvreader.CsvReader;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.util.HashMap;
import java.util.Map;

public class AddressLoad {
    public static void Run(String path,BatchInserter inserter) throws Exception {
        Label label = DynamicLabel.label("Address");
        int counter=0;

            CsvReader consumerReader = new CsvReader(path + "Neo4JAddress.csv");
            consumerReader.readHeaders();
            Long curr=System.currentTimeMillis();
            while (consumerReader.readRecord()) {
                Map<String, Object> properties = new HashMap<String, Object>();
                properties.put("Street",consumerReader.get("Street"));
                properties.put("City",consumerReader.get("City"));
                properties.put("State",consumerReader.get("State"));
                properties.put("Zip",consumerReader.get("Zip"));

                final Long node = Long.parseLong(consumerReader.get("aId"));
                inserter.createNode(node, properties, label);
                counter++;
                if(counter%1000000==0)
                {
                    System.out.print(counter/1000000);
                    System.out.print("M Addresses in ");
                    System.out.println(System.currentTimeMillis()-curr);
                    curr=System.currentTimeMillis();
                }
            }
            consumerReader.close();
            }
}
