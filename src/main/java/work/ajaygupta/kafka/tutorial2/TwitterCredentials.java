package work.ajaygupta.kafka.tutorial2;

public class TwitterCredentials {
    private String consumerKey;
    private String consumerSecret;
    private String token;
    private String secret;

    public TwitterCredentials(String consumerKey, String consumerSecret, String token, String secret) {
        this.consumerKey = consumerKey;
        this.consumerSecret = consumerSecret;
        this.token = token;
        this.secret = secret;
    }

    public String getConsumerKey() {
        return consumerKey;
    }

    public String getConsumerSecret() {
        return consumerSecret;
    }

    public String getToken() {
        return token;
    }

    public String getSecret() {
        return secret;
    }



}