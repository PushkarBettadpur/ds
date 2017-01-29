package common.messages;


public class Message implements KVMessage {

    private String key;
    private String value;
    private StatusType status;

    public Message(String key, String value, StatusType status)
    {
        this.key = key;
        this.value = value;
        this.status = status;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public StatusType getStatus() {
        return status;
    }

    public String toString() {
        String str = status + "," + key + "," + value;
        return str;
    }
}
