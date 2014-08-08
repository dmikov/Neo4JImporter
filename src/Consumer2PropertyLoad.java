import com.csvreader.CsvReader;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.util.Map;

public class Consumer2PropertyLoad {
    public static void Run(String path,BatchInserter inserter) throws Exception {
        Long curr = Main.PrintTimeStamp(0l);
        RelationshipType rType = DynamicRelationshipType.withName("has");
        int counter=0;
            CsvReader consumerReader = new CsvReader(path + "Neo4JConsumer2Property.csv");
            consumerReader.readHeaders();
            while (consumerReader.readRecord()) {
                final Long parent = Long.parseLong( consumerReader.get("parentId"));
                final Long child = Long.parseLong( consumerReader.get("childId"));
                inserter.createRelationship (parent,child,rType,null);
                counter++;
                if(counter%1000000==0)
                {
                    System.out.print(counter/1000000);
                    System.out.print("M C2P relationships in ");
                    curr=Main.PrintTimeStamp(curr);
                }
            }
            consumerReader.close();
            }
}
