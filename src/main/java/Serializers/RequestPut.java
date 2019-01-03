package Serializers;

import java.util.Map;

public class RequestPut {

    private Map<Long,byte[]> values;
    private int id;

    public Map<Long, byte[]> getValues() {
        return values;
    }

    public int getId() {
        return id;
    }

    public RequestPut(Map<Long, byte[]> values, int id) {
        this.values = values;
        this.id = id;
    }
}
