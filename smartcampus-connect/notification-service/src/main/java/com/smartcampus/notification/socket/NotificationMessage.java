// First Commit - Ahmed Abdulrahman Ahmed Ali Gamel - B032320114
// git commit -m "Add NotificationMessage TCP protocol class - Ahmed B032320114"
package com.smartcampus.notification.socket;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * ═══════════════════════════════════════════════════════════════════════════
 * R6 — DISTRIBUTED MESSAGING: Custom Message Protocol
 * ═══════════════════════════════════════════════════════════════════════════
 *
 * This class defines the custom message format used for TCP socket
 * communication between Producer services (Course Enrolment, Library
 * Booking) and the Consumer service (Notification Service).
 *
 * <h3>Message Protocol Format</h3>
 * <pre>
 *   TYPE:TIMESTAMP CONTENT
 * </pre>
 *
 * <h3>Examples</h3>
 * <pre>
 *   ENROLMENT:2026-06-10T10:30:00 Student 1 enrolled in DAD3123
 *   LIBRARY:2026-06-10T11:15:00 Student 1 borrowed book Spring Boot
 *   LIBRARY:2026-06-10T12:00:00 Student 3 booked Room LIB-ROOM-A1
 * </pre>
 *
 * <h3>Message Framing Strategy</h3>
 * <p>Each message is transmitted as a single line of UTF-8 text terminated
 * by a newline character ({@code \n}).  The consumer reads messages using
 * {@code BufferedReader.readLine()}, which blocks until a complete line
 * is received.  This line-based framing is simple, human-readable, and
 * avoids the complexity of length-prefix or delimiter-based binary
 * protocols.</p>
 *
 * <h3>Parsing</h3>
 * <p>The first colon ({@code :}) separates the TYPE from the rest.
 * The first space after the timestamp separates the TIMESTAMP from
 * the CONTENT.  This is handled by {@link #parse(String)}.</p>
 *
 * <h3>Role in R6</h3>
 * <ul>
 *   <li><b>Serialization</b>: {@link #serialize()} converts the object
 *       to the wire format for transmission over the TCP socket.</li>
 *   <li><b>Deserialization</b>: {@link #parse(String)} reconstructs
 *       the object from the wire format on the consumer side.</li>
 * </ul>
 * ═══════════════════════════════════════════════════════════════════════════
 */
public class NotificationMessage implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    private final String type;
    private final LocalDateTime timestamp;
    private final String content;

    /**
     * Creates a new notification message.
     *
     * @param type      the message type (e.g., ENROLMENT, LIBRARY)
     * @param timestamp the time the event occurred
     * @param content   the human-readable event description
     */
    public NotificationMessage(String type, LocalDateTime timestamp, String content) {
        this.type = type;
        this.timestamp = timestamp;
        this.content = content;
    }

    /**
     * Serializes this message into the custom TCP wire format.
     *
     * <p>Format: {@code TYPE:TIMESTAMP CONTENT}</p>
     * <p>Example: {@code ENROLMENT:2026-06-10T10:30:00 Student 1 enrolled in DAD3123}</p>
     *
     * @return the serialized string ready for TCP transmission
     */
    public String serialize() {
        return type + ":" + timestamp.format(FORMATTER) + " " + content;
    }

    /**
     * Parses a raw TCP message string back into a {@link NotificationMessage}.
     *
     * <p>Expected format: {@code TYPE:TIMESTAMP CONTENT}</p>
     * <p>Parsing steps:</p>
     * <ol>
     *   <li>Split on the first {@code :} to extract the TYPE.</li>
     *   <li>Split the remainder on the first space to separate TIMESTAMP from CONTENT.</li>
     * </ol>
     *
     * @param raw the raw message string received from the TCP socket
     * @return the parsed {@link NotificationMessage}
     * @throws IllegalArgumentException if the message format is invalid
     */
    public static NotificationMessage parse(String raw) {
        if (raw == null || raw.isBlank()) {
            throw new IllegalArgumentException("Cannot parse null or blank message");
        }

        // Split on first colon to get TYPE
        int colonIndex = raw.indexOf(':');
        if (colonIndex < 0) {
            throw new IllegalArgumentException("Invalid message format (missing colon): " + raw);
        }

        String type = raw.substring(0, colonIndex).trim();
        String remainder = raw.substring(colonIndex + 1).trim();

        // Split remainder on first space to get TIMESTAMP and CONTENT
        int spaceIndex = remainder.indexOf(' ');
        if (spaceIndex < 0) {
            throw new IllegalArgumentException("Invalid message format (missing content): " + raw);
        }

        String timestampStr = remainder.substring(0, spaceIndex).trim();
        String content = remainder.substring(spaceIndex + 1).trim();

        LocalDateTime timestamp = LocalDateTime.parse(timestampStr, FORMATTER);

        return new NotificationMessage(type, timestamp, content);
    }

    // Getters
    public String getType() { return type; }
    public LocalDateTime getTimestamp() { return timestamp; }
    public String getContent() { return content; }

    @Override
    public String toString() {
        return "NotificationMessage{type='" + type + "', timestamp=" + timestamp + ", content='" + content + "'}";
    }
}
