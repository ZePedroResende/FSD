package Serializers;

import java.util.Map;

public class RequestPut {

    private Map<Long,byte[]> values;

    public Map<Long, byte[]> getValues() {
        return values;
    }

    public RequestPut(Map<Long, byte[]> values) {
        this.values = values;
    }
}
