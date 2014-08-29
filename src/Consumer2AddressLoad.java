import com.csvreader.CsvReader;
import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;

public class Consumer2AddressLoad {
    public static Integer Run(String path, BatchInserter inserter, LuceneBatchInserterIndexProvider indexProvider) throws Exception {
        Integer counter = 0;
        RelationshipType mailType = DynamicRelationshipType.withName("mailing");
        RelationshipType situsType = DynamicRelationshipType.withName("situs");
        BatchInserterIndex ixa = indexProvider.nodeIndex("id", MapUtil.stringMap("type", "exact"));
        BatchInserterIndex ixp = indexProvider.nodeIndex("pin", MapUtil.stringMap("type", "exact"));
        CsvReader reader = new CsvReader(path + "Consumer2Address.csv");
        reader.setDelimiter('|');
        reader.readHeaders();
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
                final Long child = ixa.get("id", aid).getSingle();
                if(child!=null)
                    inserter.createRelationship(lastPin.GraphKey, child, type.equals("1") ? mailType : situsType, null);
            }
            counter++;
        }
        reader.close();
        return counter;
    }
}
