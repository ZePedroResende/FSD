package Serializers;

import java.util.Map;

public class ResponseGet {

    private Map<Long,byte[]> response;

    public Map<Long, byte[]> getResponse() {
        return response;
    }

    public ResponseGet(Map<Long,byte[]> response) {
        this.response = response;
    }
}
