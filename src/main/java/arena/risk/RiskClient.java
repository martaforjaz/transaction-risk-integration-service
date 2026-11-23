package arena.risk;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

@Component
class RiskClient {
    private final RestClient client;
    private final String url;

    RiskClient(@Value("${risk.url}") String url) {
        this.url = url;
        var config = new JdkClientHttpRequestFactory(java.net.http.HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(2)).build());
        config.setReadTimeout(Duration.ofSeconds(2));
        client = RestClient.builder().requestFactory(config).build();
    }

    String evaluate(UUID id, long cents, String currency) {
        var response = client.post().uri(url)
                .header("Idempotency-Key", id.toString())
                .body(Map.of("id", id, "amountMinor", cents, "currency", currency))
                .retrieve().body(Result.class);
        if (response == null || !("ALLOW".equals(response.decision()) || "REVIEW".equals(response.decision()))) {
            throw new RestClientException("Invalid risk response");
        }
        return response.decision();
    }

    record Result(String decision) {}
}

