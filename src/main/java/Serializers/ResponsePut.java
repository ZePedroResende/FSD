package Serializers;

public class ResponsePut {

    private Boolean response;
    private int id;

    public Boolean getResponse() {
        return response;
    }

    public ResponsePut(Boolean response, int id ) {
        this.response = response;
        this.id = id;
    }
    public int getId() {
        return id;
    }

}
