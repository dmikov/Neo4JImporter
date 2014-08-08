import com.csvreader.CsvReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;


/**
 * Created with IntelliJ IDEA.
 * User: dkrasnikov
 * Date: 10/10/13
 * Time: 11:04 AM
 * To change this template use File | Settings | File Templates.
 */
public class Reader {
    protected final CsvReader _reader;
    protected final String[] _headers;
    private String _lastKey;

    public Reader(String path) throws IOException {
        _reader= new CsvReader(path);
        _reader.readHeaders();
        _headers=_reader.getHeaders();
        GetValue();
    }
    protected boolean GetValue() throws IOException {
        if(_reader.readRecord())
        {_lastKey=_reader.get("cId");
            return true;}
        else{return false;}
    }
    protected LinkedHashMap<String, String> GetObject() throws IOException {
        LinkedHashMap<String,String> result = new LinkedHashMap<String, String>();
        for (String field : _headers)
            if(!field.equals("cId"))
                result.put(field,_reader.get(field));
        return result;
    }
    public MyTable Get(String key) throws IOException {
        if(!_lastKey.equals(key))
            return null;
        MyTable result = new MyTable();
        result.add(GetObject());
        while (GetValue() && _lastKey.equals(key))
            result.add(GetObject());
        return result;
    }
}
