package cl.ochodigital.pasteleriamydreams.estadisticasservice.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;

import java.net.URI;

// Cliente de Kinesis configurable (Entrega 3): LocalStack en local (endpoint +
// credenciales estáticas de prueba) y el Kinesis real del lab en AWS (endpoint
// vacío + la cadena de credenciales estándar de AWS). Mismo patrón que en
// pedidos-service: el código no cambia al migrar, cambia la configuración.
@Configuration
public class KinesisConfig {

    // Endpoint vacío = AWS real (usa credenciales del entorno/rol)
    @Value("${app.kinesis.endpoint:}")
    private String endpoint;

    @Value("${app.kinesis.region:us-east-1}")
    private String region;

    @Bean
    public KinesisClient kinesisClient() {
        var builder = KinesisClient.builder()
                .region(Region.of(region))
                .httpClientBuilder(UrlConnectionHttpClient.builder());
        if (endpoint != null && !endpoint.isBlank()) {
            // LocalStack: cualquier credencial sirve, solo importa la URL
            builder.endpointOverride(URI.create(endpoint))
                    .credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create("test", "test")));
        }
        return builder.build();
    }
}
