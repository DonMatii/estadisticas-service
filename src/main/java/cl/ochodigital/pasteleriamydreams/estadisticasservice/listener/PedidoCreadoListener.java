package cl.ochodigital.pasteleriamydreams.estadisticasservice.listener;

import cl.ochodigital.pasteleriamydreams.estadisticasservice.event.PedidoCreadoEvent;
import cl.ochodigital.pasteleriamydreams.estadisticasservice.repository.EstadisticasRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

// Consume el topic `pedidos` y alimenta las estadísticas con pedidos reales (RF-10)
@Component
public class PedidoCreadoListener {

    private static final Logger log = LoggerFactory.getLogger(PedidoCreadoListener.class);

    private final EstadisticasRepository repositorio;
    private final ObjectMapper objectMapper;

    public PedidoCreadoListener(EstadisticasRepository repositorio, ObjectMapper objectMapper) {
        this.repositorio = repositorio;
        this.objectMapper = objectMapper;
    }

    // Grupo de consumidor "estadisticas"; el topic viene de app.kafka.topic (default "pedidos")
    @KafkaListener(groupId = "estadisticas", topics = "${app.kafka.topic:pedidos}")
    public void onPedidoCreado(String mensaje) {
        try {
            PedidoCreadoEvent evento = objectMapper.readValue(mensaje, PedidoCreadoEvent.class);
            if (evento == null || evento.id() == null) {
                log.error("Evento sin id, se aísla en la DLQ: {}", mensaje);
                throw new IllegalArgumentException("evento sin id de pedido");
            }
            // La venta se atribuye al dia del PEDIDO (evento.fecha), no al dia en que el
            // consumidor la procesa: con reentrega o replay de Kafka, usar LocalDate.now()
            // desplazaba la venta al dia equivocado y ventas_diarias se desincronizaba
            boolean esNuevo = repositorio.procesarPedido(
                    evento.id(), BigDecimal.valueOf(evento.total()), evento.fecha().toLocalDate());
            if (esNuevo) {
                log.info("Pedido {} incorporado a las estadísticas", evento.id());
            } else {
                log.warn("Pedido {} ya estaba procesado: se omite el doble conteo", evento.id());
            }
        } catch (JsonProcessingException | IllegalArgumentException e) {
            // Falla DEFINITIVA: payload ilegible o evento inválido -> sin
            // reintentos, el error handler lo aísla en la DLQ para revisión
            log.error("Mensaje definitivamente inválido, va a la DLQ. Payload: {} - Razón: {}",
                    mensaje, e.getMessage());
            throw new IllegalArgumentException("evento de pedido inválido", e);
        } catch (Exception e) {
            // Falla TRANSITORIA (base caída, infra): se relanza para que el
            // error handler reintente y, solo si persiste, lo lleve a la DLQ.
            // El "log y skip" anterior perdía la venta en silencio.
            log.error("Fallo recuperable procesando el evento. Payload: {} - Razón: {}",
                    mensaje, e.getMessage());
            throw e instanceof RuntimeException ? (RuntimeException) e : new IllegalStateException(e);
        }
    }
}
