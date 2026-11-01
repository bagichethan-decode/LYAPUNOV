# Dead-letter queue operations

LYAPUNOV stores terminal delivery failures in `outbox_events` with status `DEAD`.
Legacy rows with status `FAILED` are also visible to the operator listing command.

## Configure database access

Set these environment variables in the same PowerShell session used to run Maven:

```powershell
$env:LYAPUNOV_JDBC_URL = "jdbc:postgresql://localhost:5432/lyapunov"
$env:LYAPUNOV_DB_USER = "postgres"
$env:LYAPUNOV_DB_PASSWORD = "your-local-password"
```

Use the credentials configured in your local Docker Compose environment. Do not commit credentials.

## Build

```powershell
.\mvnw.cmd -B -ntp clean verify
```

## Run the CLI

The CLI is a Java main class. Run it with the project's compiled classes and Maven dependency classpath.

```powershell
.\mvnw.cmd -q dependency:build-classpath "-Dmdep.outputFile=target\classpath.txt"
$classpath = "target\classes;" + (Get-Content target\classpath.txt -Raw)
java -cp $classpath com.lyapunov.cli.DeadLetterQueueCli list
```

List supports a page size from 1 to 500 and a zero-based offset:

```powershell
java -cp $classpath com.lyapunov.cli.DeadLetterQueueCli list 25 0
java -cp $classpath com.lyapunov.cli.DeadLetterQueueCli list 25 25
```

## Resolve a terminal event

Copy an event UUID from the list output. Record a meaningful operator ID and reason:

```powershell
java -cp $classpath com.lyapunov.cli.DeadLetterQueueCli retry "<event-uuid>" "operator-1" "Fixed downstream mapping"
```

Or explicitly skip an event:

```powershell
java -cp $classpath com.lyapunov.cli.DeadLetterQueueCli skip "<event-uuid>" "operator-1" "Approved by incident review"
```

Retry resets the retry counter and requeues the event. Skip marks the event `SKIPPED`, allowing later sequence numbers for that aggregate to proceed. Both actions are transactional and recorded in `event_history`.

## Operational cautions

- Inspect the event and its downstream failure before retrying.
- Use `skip` only after an explicit business/operator decision; it changes aggregate progression.
- The CLI does not delete events or event history.
- Delivery remains at-least-once. A successful retry can still be observed more than once downstream; consumers should be idempotent.