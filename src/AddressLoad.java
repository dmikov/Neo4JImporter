import com.csvreader.CsvReader;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;

import java.util.HashMap;
import java.util.Map;

public class AddressLoad {
    public static Integer Run(String path, BatchInserter inserter, BatchInserterIndexProvider indexProvider) throws Exception {
        Integer counter = 0;
        Label label = DynamicLabel.label("Address");
        BatchInserterIndex ix = indexProvider.nodeIndex("id", MapUtil.stringMap("type", "exact"));
        CsvReader reader = new CsvReader(path + "Address.csv");
        reader.setDelimiter('|');
        reader.readHeaders();
        while (reader.readRecord()) {
            Map<String, Object> properties = new HashMap<String, Object>();
            properties.put("Street", reader.get("Street"));
            properties.put("City", reader.get("City"));
            properties.put("State", reader.get("State"));
            properties.put("Zip", reader.get("Zip"));
            final String id = reader.get("Id");
            properties.put("Id", id);

            long node = inserter.createNode(properties, label);
            ix.add(node, MapUtil.map("id", id));
            counter++;
        }
        ix.flush();
        reader.close();
        return counter;
    }
}
