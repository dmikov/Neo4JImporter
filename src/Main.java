import com.csvreader.CsvReader;
import org.neo4j.cypher.internal.*;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws Exception {
        Long curr = PrintTimeStamp(0l);
//        Map<String, String> settings = MapUtil.load(new File("e:\\neo4j\\conf\\batch.properties"));
        BatchInserter inserter = BatchInserters.inserter("/home/dkrasnikov/data/graph.db");

        String path = args[0];
        ConsumerLoad.Run(path, inserter);
        curr=PrintTimeStamp(curr);
        AddressLoad.Run(path,inserter);
        curr=PrintTimeStamp(curr);
//        PropertyLoad.Run(path,inserter);
//        Consumer2AddressLoad.Run(path,inserter);
//        Consumer2PropertyLoad.Run(path,inserter);


        System.out.println("Initializing shutdown");
        inserter.shutdown();
        PrintTimeStamp(curr);
    }
public static Long PrintTimeStamp(Long curr) {
    System.out.println(System.currentTimeMillis()-curr);
    curr=System.currentTimeMillis();
    return curr;
}
}