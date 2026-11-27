package com.lyapunov.cli;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class LyapunovCliTest {

    private CommandLine commandLine(ByteArrayOutputStream output,
                                    ByteArrayOutputStream errors) {
        CommandLine commandLine = new CommandLine(new LyapunovCli());
        commandLine.setOut(new PrintWriter(output, true, StandardCharsets.UTF_8));
        commandLine.setErr(new PrintWriter(errors, true, StandardCharsets.UTF_8));
        return commandLine;
    }

    @Test
    void rootHelpIsAvailable() {
        var output = new ByteArrayOutputStream();
        var errors = new ByteArrayOutputStream();

        int exitCode = commandLine(output, errors).execute("--help");

        assertEquals(0, exitCode);
        assertTrue(output.toString(StandardCharsets.UTF_8).contains("lyapunov"));
        assertTrue(output.toString(StandardCharsets.UTF_8).contains("status"));
        assertTrue(errors.toString(StandardCharsets.UTF_8).isEmpty());
    }

    @Test
    void versionIsAvailable() {
        var output = new ByteArrayOutputStream();
        var errors = new ByteArrayOutputStream();

        int exitCode = commandLine(output, errors).execute("--version");

        assertEquals(0, exitCode);
        assertTrue(output.toString(StandardCharsets.UTF_8).contains("1.0.0"));
    }

    @Test
    void eventsHelpListsAvailableCommands() {
        var output = new ByteArrayOutputStream();
        var errors = new ByteArrayOutputStream();

        int exitCode = commandLine(output, errors).execute("events", "--help");

        assertEquals(0, exitCode);
        assertTrue(output.toString(StandardCharsets.UTF_8).contains("dead"));
        assertTrue(output.toString(StandardCharsets.UTF_8).contains("create"));
    }

    @Test
    void createRequiresAllEventFields() {
        var output = new ByteArrayOutputStream();
        var errors = new ByteArrayOutputStream();

        int exitCode = commandLine(output, errors)
            .execute("events", "create");

        assertNotEquals(0, exitCode);
        assertTrue(errors.toString(StandardCharsets.UTF_8)
            .contains("--aggregate-type"));
    }

    @Test
    void deadEventsRejectInvalidLimit() {
        var output = new ByteArrayOutputStream();
        var errors = new ByteArrayOutputStream();

        int exitCode = commandLine(output, errors)
            .execute("events", "dead", "--limit", "501");

        assertEquals(2, exitCode);
        assertTrue(errors.toString(StandardCharsets.UTF_8).isEmpty());
    }
}
