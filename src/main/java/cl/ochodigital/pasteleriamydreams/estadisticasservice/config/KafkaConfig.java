package cl.ochodigital.pasteleriamydreams.estadisticasservice.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.FixedBackOff;

// Política de errores del consumidor de la cola (rúbrica: éxito / error
// recuperable / error definitivo):
//  - TRANSITORIA (base o infraestructura caída) -> reintentos con backoff fijo
//    (app.kafka.reintentos.*) y solo después de agotarlos el mensaje va a la DLQ.
//  - DEFINITIVA (payload ilegible, evento inválido) -> sin reintentos, directo
//    a la DLQ.
//  - Todo mensaje que llega a la DLQ conserva el payload original y los
//    headers con la falla, para revisión y reproceso manual. Nada se pierde
//    en silencio. Los duplicados no llegan acá: el guard de idempotencia los
//    salta antes (control de duplicados).
@Configuration
public class KafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    // Reintentos después del primer intento (2 = hasta 3 intentos en total)
    @Value("${app.kafka.reintentos.max:2}")
    private int maxReintentos;

    // Espera entre reintentos en milisegundos (configurable por entorno)
    @Value("${app.kafka.reintentos.espera-ms:1000}")
    private long esperaEntreReintentosMs;

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory, KafkaTemplate<String, String> kafkaTemplate) {

        // Los mensajes fallidos terminan en <topic>.DLT (p. ej. pedidos.DLT)
        // con el motivo de la falla en los headers; el aislamiento queda en log
        DeadLetterPublishingRecoverer publicadorDlt = new DeadLetterPublishingRecoverer(kafkaTemplate) {
            @Override
            public void accept(org.apache.kafka.clients.consumer.ConsumerRecord<?, ?> registro, Exception error) {
                log.error("Mensaje aislado en la DLQ: topic={} partición={} offset={} - Razón: {}",
                        registro.topic(), registro.partition(), registro.offset(), error.getMessage());
                super.accept(registro, error);
            }
        };

        DefaultErrorHandler manejador = new DefaultErrorHandler(publicadorDlt,
                new FixedBackOff(esperaEntreReintentosMs, maxReintentos));

        // Fallas definitivas: sin reintentos, directo a la DLQ
        manejador.addNotRetryableExceptions(
                JsonProcessingException.class,
                IllegalArgumentException.class,
                DeserializationException.class);

        // Trazabilidad: cada reintento y la entrega final a la DLQ quedan en log
        manejador.setRetryListeners((registro, error, intento) ->
                log.warn("Reintento #{} para el mensaje de {} (clave {}): {}",
                        intento, registro.topic(), registro.key(), error.getMessage()));

        ConcurrentKafkaListenerContainerFactory<String, String> fabrica =
                new ConcurrentKafkaListenerContainerFactory<>();
        fabrica.setConsumerFactory(consumerFactory);
        fabrica.setCommonErrorHandler(manejador);
        return fabrica;
    }
}
