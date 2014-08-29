import com.csvreader.CsvReader;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;

import java.util.*;

public class ConsumerLoad {
    public static Integer Run(String path, BatchInserter inserter, LuceneBatchInserterIndexProvider indexProvider) throws Exception {
        Integer counter = 0;
        BatchInserterIndex iNames = indexProvider.nodeIndex("names", MapUtil.stringMap("type", "exact"));
        BatchInserterIndex iPin = indexProvider.nodeIndex("pin", MapUtil.stringMap("type", "exact"));
        Label personLabel = DynamicLabel.label("Consumer");
        CsvReader reader = new CsvReader(path + "Consumer.csv");
        reader.setDelimiter('|');
        reader.readHeaders();
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
                iNames.add(node, MapUtil.map("names", name));
            iPin.add(node, MapUtil.map("pin", consumerPin));
            counter++;
        }
        iNames.flush();
        iPin.flush();
        reader.close();
        return counter;
    }
}
