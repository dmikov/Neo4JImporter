import com.csvreader.CsvReader;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.util.HashMap;
import java.util.Map;

public class Consumer2AddressLoad {
    public static void Run(String path,BatchInserter inserter) throws Exception {
        RelationshipType rType = DynamicRelationshipType.withName("in");
        int counter=0;
            CsvReader consumerReader = new CsvReader(path + "Neo4JConsumer2Address.csv");
            consumerReader.readHeaders();
            Long curr=System.currentTimeMillis();
            while (consumerReader.readRecord()) {
                final Long parent = Long.parseLong( consumerReader.get("cId"));
                final Long child = Long.parseLong( consumerReader.get("aId"));
                inserter.createRelationship(parent,child,rType,null);
                counter++;
                if(counter%1000000==0)
                {
                    System.out.print(counter/1000000);
                    System.out.print("M C2A relationships in ");
                    curr = Main.PrintTimeStamp(curr);
                }
            }
            consumerReader.close();
            }
}
