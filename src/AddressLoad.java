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
        BatchInserterIndexProvider indexProvider = new LuceneBatchInserterIndexProvider(inserter);
        BatchInserterIndex ix =indexProvider.nodeIndex("id", MapUtil.stringMap("type","exact"));
        int counter=0;

        CsvReader reader = new CsvReader(path + "Address.csv");
        reader.readHeaders();
        Long curr=System.currentTimeMillis();
        while (reader.readRecord()) {
            Map<String, Object> properties = new HashMap<String, Object>();
            properties.put("Street",reader.get("Street"));
            properties.put("City",reader.get("City"));
            properties.put("State",reader.get("State"));
            properties.put("Zip",reader.get("Zip"));
            final String id = reader.get("Id");
            properties.put("Id", id);

            long node = inserter.createNode(properties, label);
            ix.add(node,MapUtil.map("id",id));
            counter++;
            if(counter%1000000==0)
            {
                ix.flush();
                System.out.print(counter/1000000);
                System.out.print("M Addresses in ");
                System.out.println(System.currentTimeMillis()-curr);
                curr=System.currentTimeMillis();
            }
        }
        indexProvider.shutdown();
        reader.close();
        }
}
