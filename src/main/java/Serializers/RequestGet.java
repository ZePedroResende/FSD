package Serializers;

import java.util.Collection;

public class RequestGet {

    private Collection<Long> values;

    public Collection<Long> getValues() {
        return values;
    }

    public RequestGet(Collection<Long> values) {
        this.values = values;
    }
}
