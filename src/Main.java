//import com.csvreader.CsvReader;
//import org.neo4j.cypher.internal.*;
//import org.neo4j.graphdb.DynamicLabel;
//import org.neo4j.graphdb.DynamicRelationshipType;
//import org.neo4j.graphdb.Label;
//import org.neo4j.graphdb.RelationshipType;
//import org.neo4j.graphdb.schema.IndexCreator;
//import org.neo4j.helpers.collection.MapUtil;

import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

public class Main {
    public static void main(String[] args) throws Exception {
        Long curr = PrintTimeStamp(0l);
        String path = args[0];
//        Map<String, String> settings = MapUtil.load(new File("e:\\neo4j\\conf\\batch.properties"));
        final String storeDir = "/home/dkrasnikov/data/graph.db";
        final BatchInserter inserter = BatchInserters.inserter(storeDir);
        final LuceneBatchInserterIndexProvider indexProvider = new LuceneBatchInserterIndexProvider(inserter);

        wl("Loading Customers");
        final Integer consumerRun = ConsumerLoad.Run(path, inserter, indexProvider);
        w(consumerRun.toString() + " in ");
        curr = PrintTimeStamp(curr);

        wl("Loading Addresses.");
        final Integer addressRun = AddressLoad.Run(path, inserter, indexProvider);
        w(addressRun.toString() + " in ");
        curr = PrintTimeStamp(curr);

        wl("Loading Consumer to Address relationships.");
        final Integer c2aRun = Consumer2AddressLoad.Run(path, inserter, indexProvider);
        w(c2aRun.toString() + " in ");
        curr = PrintTimeStamp(curr);

//        PropertyLoad.Run(path,inserter);
//        Consumer2PropertyLoad.Run(path,inserter);


        System.out.println("Initializing shutdown");
        indexProvider.shutdown();
        inserter.shutdown();
        PrintTimeStamp(curr);
    }

    public static Long PrintTimeStamp(Long curr) {
        System.out.println(System.currentTimeMillis() - curr);
        curr = System.currentTimeMillis();
        return curr;
    }

    public static void w(String value) {
        System.out.print(value);
    }

    public static void wl(String value) {
        System.out.println(value);
    }
}