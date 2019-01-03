package Serializers;

import java.util.Map;

public class ResponseGet {

    private Map<Long,byte[]> response;
    private int id;

    public int getId() {
        return id;
    }

    public Map<Long, byte[]> getResponse() {
        return response;
    }

    public ResponseGet(Map<Long,byte[]> response, int id) {
        this.response = response;
        this.id= id;
    }

}
