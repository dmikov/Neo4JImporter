//import com.csvreader.CsvReader;
//import org.neo4j.cypher.internal.*;
//import org.neo4j.graphdb.DynamicLabel;
//import org.neo4j.graphdb.DynamicRelationshipType;
//import org.neo4j.graphdb.Label;
//import org.neo4j.graphdb.RelationshipType;
//import org.neo4j.graphdb.schema.IndexCreator;
//import org.neo4j.helpers.collection.MapUtil;

import com.csvreader.CsvReader;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.util.HashMap;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws Exception {
        String path = args[0];
        Loader loader=new Loader(path);
        loader.Run();
    }


}