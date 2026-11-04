package com.lyapunov.inbox;

import org.junit.jupiter.api.Test;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class InboxKeyTest {
    @Test void acceptsConsumerAndEventIdentity() {
        UUID id = UUID.randomUUID();
        InboxKey key = new InboxKey("orders-projection", id);
        assertEquals("orders-projection", key.consumerName());
        assertEquals(id, key.eventId());
    }
    @Test void rejectsBlankConsumerName() {
        assertThrows(IllegalArgumentException.class, () -> new InboxKey("  ", UUID.randomUUID()));
    }
    @Test void rejectsNullEventId() {
        assertThrows(NullPointerException.class, () -> new InboxKey("orders-projection", null));
    }
}
