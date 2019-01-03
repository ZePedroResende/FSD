package Serializers;

import java.util.Collection;

public class RequestGet {

    private Collection<Long> values;
    private int id;

    public Collection<Long> getValues() {
        return values;
    }

    public RequestGet(Collection<Long> values, int id) {
        this.values = values;
        this.id = id;
    }
    public int getId() {
        return id;
    }
}
