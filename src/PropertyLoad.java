import com.csvreader.CsvReader;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import java.util.HashMap;
import java.util.Map;

public class PropertyLoad {
    public static void Run(String path,BatchInserter inserter) throws Exception {
        Long curr=System.currentTimeMillis();
        Label label = DynamicLabel.label("Property");
        int counter=0;
            CsvReader consumerReader = new CsvReader(path + "Neo4JProperty.csv");
            consumerReader.readHeaders();
            while (consumerReader.readRecord()) {
                Map<String, Object> properties = new HashMap<String, Object>();
                properties.put("CntyCd",consumerReader.get("CntyCd"));
                properties.put("PclId",consumerReader.get("PclId"));
                properties.put("PclSeq",consumerReader.get("PclSeq"));
                final Long node = Long.parseLong(consumerReader.get("pId"));
                inserter.createNode(node, properties, label);
                counter++;
                if(counter%1000000==0)
                {
                    System.out.print(counter/1000000);
                    System.out.print("M Properties in ");
                    curr = PrintTimeStamp(curr);
                }
            }
            consumerReader.close();
            }

    private static Long PrintTimeStamp(Long curr) {
        System.out.println(System.currentTimeMillis()-curr);
        curr=System.currentTimeMillis();
        return curr;
    }
}
