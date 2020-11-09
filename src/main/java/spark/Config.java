package spark;

public class Config {

    public static final String KAFKA_URL = "10.66.1.165:9093,10.66.1.166:9093,10.66.1.167:9093" +
            ",10.66.1.46:9093,10.66.1.47:9093";

    public static final String KAFKA_TOPIC = "DS.Input.All.CSemantic";

    public static final String MONGO_HOST = "10.66.188.17";

    public static final Integer MONGO_PORT = 27017;

    public static final String MONGO_DB = "semantic";

    public static final int DURATION_QUERYCOUNT = 300;

    public static final int DURATION_USERQUERY = 600;

    public static final int ROUND = 12;

    public static final int MIN_PER_ROUND = 120;

}
