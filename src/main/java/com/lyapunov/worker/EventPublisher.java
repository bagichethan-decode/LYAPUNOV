package com.lyapunov.worker;

import com.lyapunov.model.OutboxEvent;

public interface EventPublisher extends AutoCloseable {
    void publish(OutboxEvent event) throws Exception;
    @Override
    void close() throws Exception;
}