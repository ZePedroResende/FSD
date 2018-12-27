package Serializers;

public interface Transaction {

    int getId();

    boolean isCommit();

    boolean isRollback();

}
