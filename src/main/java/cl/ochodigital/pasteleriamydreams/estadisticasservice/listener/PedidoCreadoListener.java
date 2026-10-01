package cl.ochodigital.pasteleriamydreams.estadisticasservice.listener;

import cl.ochodigital.pasteleriamydreams.estadisticasservice.event.PedidoCreadoEvent;
import cl.ochodigital.pasteleriamydreams.estadisticasservice.repository.EstadisticasRepository;
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
            boolean esNuevo = repositorio.procesarPedido(evento.id(), BigDecimal.valueOf(evento.total()));
            if (esNuevo) {
                log.info("Pedido {} incorporado a las estadísticas", evento.id());
            } else {
                log.debug("Pedido {} ya estaba procesado: se omite el doble conteo", evento.id());
            }
        } catch (Exception e) {
            // Tolerancia a veneno: un mensaje ilegible o no procesable se registra y se salta,
            // nunca se lanza excepción para afuera y el consumidor sigue vivo
            log.warn("Evento de pedido no procesable omitido: {}", e.getMessage());
        }
    }
}
