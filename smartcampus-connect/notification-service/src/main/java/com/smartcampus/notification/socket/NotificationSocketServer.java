// First Commit - Ahmed Abdulrahman Ahmed Ali Gamel - B032320114
// git commit -m "Add NotificationSocketServer TCP consumer - Ahmed B032320114"
package com.smartcampus.notification.socket;

import com.smartcampus.notification.service.NotificationService;
import jakarta.annotation.PreDestroy;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * ═══════════════════════════════════════════════════════════════════════════
 * R6 — DISTRIBUTED MESSAGING: TCP Socket Consumer (Notification Server)
 * ═══════════════════════════════════════════════════════════════════════════
 *
 * This class implements the <b>Consumer</b> side of the Producer–Consumer
 * architecture using Java TCP sockets (per Lab 5).
 *
 * <h3>Architecture Role</h3>
 * <pre>
 *   ┌───────────────────────┐          TCP (port 9999)          ┌─────────────────────────┐
 *   │  Course Enrolment     │ ──────────────────────────────────>│                         │
 *   │  Service (PRODUCER)   │         ENROLMENT messages        │   Notification Service  │
 *   └───────────────────────┘                                   │   (CONSUMER)            │
 *                                                               │                         │
 *   ┌───────────────────────┐          TCP (port 9999)          │   NotificationSocket    │
 *   │  Library Booking      │ ──────────────────────────────────>│   Server                │
 *   │  Service (PRODUCER)   │         LIBRARY messages          │                         │
 *   └───────────────────────┘                                   └─────────────────────────┘
 * </pre>
 *
 * <h3>How It Works</h3>
 * <ol>
 *   <li>On application startup ({@link ApplicationReadyEvent}), a
 *       {@link ServerSocket} begins listening on port <b>9999</b>.</li>
 *   <li>The accept loop runs on a dedicated daemon thread so it does
 *       <b>not block</b> the Spring Boot startup sequence.</li>
 *   <li>For each producer that connects, a new handler thread is
 *       dispatched from the {@link ExecutorService} thread pool.</li>
 *   <li>Each handler reads messages line-by-line using
 *       {@link BufferedReader#readLine()}, parses them via
 *       {@link NotificationMessage#parse(String)}, and persists them
 *       to the database through {@link NotificationService}.</li>
 *   <li>The handler continues reading until the producer disconnects
 *       (i.e., {@code readLine()} returns {@code null}).</li>
 * </ol>
 *
 * <h3>Thread Pool</h3>
 * <p>A fixed thread pool of <b>5</b> threads is used to handle concurrent
 * producer connections.  This means up to 5 producer microservices can
 * connect simultaneously without spawning unbounded threads.</p>
 *
 * <h3>Message Framing</h3>
 * <p>Messages use <b>line-based framing</b> — each message is a single
 * line of UTF-8 text terminated by {@code \n}.  The reader uses
 * {@code BufferedReader.readLine()} which blocks until a full line
 * arrives, then returns the content without the newline.</p>
 *
 * <h3>Lifecycle</h3>
 * <ul>
 *   <li><b>Startup</b>: Triggered by {@code ApplicationReadyEvent} — the
 *       server socket starts after Spring has fully initialized.</li>
 *   <li><b>Shutdown</b>: {@code @PreDestroy} closes the server socket
 *       and shuts down the thread pool gracefully.</li>
 * </ul>
 * ═══════════════════════════════════════════════════════════════════════════
 */
@Component
public class NotificationSocketServer {

    /** The TCP port the consumer server listens on. */
    private static final int PORT = 9999;

    private final NotificationService notificationService;
    private final ExecutorService clientHandlerPool;
    private ServerSocket serverSocket;
    private volatile boolean running = true;

    // Constructor injection (no Lombok, consistent with project style)
    public NotificationSocketServer(NotificationService notificationService) {
        this.notificationService = notificationService;
        // Fixed thread pool for handling concurrent producer connections
        this.clientHandlerPool = Executors.newFixedThreadPool(5);
    }

    /**
     * R6 — Starts the TCP socket server after Spring Boot has fully started.
     *
     * <p>Uses {@link ApplicationReadyEvent} so the server does not start
     * until all beans are initialized and the HTTP server is ready.
     * The accept loop runs on a separate daemon thread to avoid blocking
     * the main Spring Boot thread.</p>
     */
    @EventListener(ApplicationReadyEvent.class)
    public void startServer() {
        Thread serverThread = new Thread(() -> {
            try {
                serverSocket = new ServerSocket(PORT);
                System.out.println("[NotificationSocketServer] TCP Consumer started on port " + PORT);
                System.out.println("[NotificationSocketServer] Waiting for producer connections...");

                while (running) {
                    try {
                        // Accept blocks until a producer connects
                        Socket clientSocket = serverSocket.accept();
                        String clientAddress = clientSocket.getRemoteSocketAddress().toString();
                        System.out.println("[NotificationSocketServer] Producer connected: " + clientAddress);

                        // Dispatch the connection to a pooled thread
                        clientHandlerPool.submit(() -> handleProducer(clientSocket, clientAddress));

                    } catch (Exception e) {
                        if (running) {
                            System.err.println("[NotificationSocketServer] Error accepting connection: " + e.getMessage());
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("[NotificationSocketServer] Failed to start server: " + e.getMessage());
            }
        });

        // Daemon thread — JVM can exit even if this thread is still running
        serverThread.setDaemon(true);
        serverThread.setName("notification-socket-server");
        serverThread.start();
    }

    /**
     * Handles a single producer connection.
     *
     * <p>Reads messages line-by-line from the TCP socket, parses each
     * message using the {@link NotificationMessage} protocol, and saves
     * the notification to the database.  Continues reading until the
     * producer disconnects (readLine returns null).</p>
     *
     * @param clientSocket  the connected producer's socket
     * @param clientAddress the producer's network address (for logging)
     */
    private void handleProducer(Socket clientSocket, String clientAddress) {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(clientSocket.getInputStream()))) {

            String rawMessage;
            // Read messages continuously until the producer disconnects
            while ((rawMessage = reader.readLine()) != null) {
                try {
                    // Parse the raw TCP message into a structured object
                    NotificationMessage message = NotificationMessage.parse(rawMessage);

                    // Save the notification to the database
                    notificationService.saveNotification(message.getType(), message.getContent());

                    System.out.println("[NotificationSocketServer] Received and saved: "
                            + message.getType() + " -> " + message.getContent());

                } catch (Exception e) {
                    System.err.println("[NotificationSocketServer] Failed to process message: "
                            + rawMessage + " | Error: " + e.getMessage());
                }
            }

            System.out.println("[NotificationSocketServer] Producer disconnected: " + clientAddress);

        } catch (Exception e) {
            System.err.println("[NotificationSocketServer] Connection error with "
                    + clientAddress + ": " + e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (Exception e) {
                // Ignore close errors
            }
        }
    }

    /**
     * R6 — Graceful shutdown of the TCP server and thread pool.
     *
     * <p>Called automatically by Spring when the application context
     * is destroyed.  Closes the server socket and shuts down the
     * client handler thread pool to prevent thread leaks.</p>
     */
    @PreDestroy
    public void stopServer() {
        running = false;
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (Exception e) {
            System.err.println("[NotificationSocketServer] Error closing server socket: " + e.getMessage());
        }

        clientHandlerPool.shutdown();
        try {
            if (!clientHandlerPool.awaitTermination(10, TimeUnit.SECONDS)) {
                clientHandlerPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            clientHandlerPool.shutdownNow();
            Thread.currentThread().interrupt();
        }

        System.out.println("[NotificationSocketServer] TCP Consumer stopped.");
    }
}
