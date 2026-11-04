package com.lyapunov.inbox;

import java.sql.Connection;
import java.sql.SQLException;

/** Business database work that must use the supplied inbox transaction. */
@FunctionalInterface
public interface InboxWork {
    void execute(Connection connection) throws SQLException;
}
