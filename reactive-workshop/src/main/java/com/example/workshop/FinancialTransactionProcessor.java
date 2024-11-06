package com.example.workshop;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Random;

public class FinancialTransactionProcessor {
    private static final Logger logger = LoggerFactory.getLogger(FinancialTransactionProcessor.class);
    private static final Random random = new Random();

    public static void main(String[] args) throws InterruptedException {
        // Simular un flujo de transacciones financieras
        Flux<Transaction> transactionStream = Flux.interval(Duration.ofMillis(100))
                .map(i -> generateRandomTransaction())
                .take(50);

        transactionStream
                // 1. Filtrar transacciones válidas
                .filter(FinancialTransactionProcessor::isValidTransaction)
                
                // 2. Enriquecer la transacción con información adicional
                .map(FinancialTransactionProcessor::enrichTransaction)
                
                // 3. Categorizar y procesar las transacciones
                .groupBy(Transaction::getType)
                
                // 4. Procesar cada categoría por separado
                .flatMap(group -> group
                        .publishOn(Schedulers.boundedElastic())
                        .map(FinancialTransactionProcessor::processTransaction)
                        .buffer(Duration.ofSeconds(5))
                        .map(transactions -> generateReport(group.key(), transactions))
                )
                
                // 5. Manejar errores
                .onErrorResume(error -> {
                    logger.error("Error en el procesamiento: " + error.getMessage());
                    return Flux.empty();
                })
                
                // 6. Limitar la tasa de procesamiento
                .sample(Duration.ofSeconds(1))
                
                .subscribe(
                        report -> logger.info(report),
                        error -> logger.error("Error crítico: " + error.getMessage()),
                        () -> logger.info("Procesamiento de transacciones completado")
                );

        // Esperar a que se completen todas las operaciones
        Thread.sleep(30000);
    }

    private static Transaction generateRandomTransaction() {
        String[] types = {"PAYMENT", "TRANSFER", "DEPOSIT", "WITHDRAWAL"};
        return new Transaction(
                random.nextLong(1000000),
                types[random.nextInt(types.length)],
                random.nextDouble() * 1000,
                random.nextBoolean()
        );
    }

    private static boolean isValidTransaction(Transaction transaction) {
        return transaction.isValid() && transaction.getAmount() > 0;
    }

    private static Transaction enrichTransaction(Transaction transaction) {
        transaction.setProcessedTimestamp(System.currentTimeMillis());
        return transaction;
    }

    private static Transaction processTransaction(Transaction transaction) {
        try {
            Thread.sleep(random.nextInt(100));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        transaction.setProcessed(true);
        return transaction;
    }

    private static String generateReport(String type, java.util.List<Transaction> transactions) {
        double total = transactions.stream().mapToDouble(Transaction::getAmount).sum();
        return String.format("Reporte para %s: %d transacciones procesadas, total $%.2f", 
                           type, transactions.size(), total);
    }

    static class Transaction {
        private long id;
        private String type;
        private double amount;
        private boolean valid;
        private long processedTimestamp;
        private boolean processed;

        Transaction(long id, String type, double amount, boolean valid) {
            this.id = id;
            this.type = type;
            this.amount = amount;
            this.valid = valid;
        }

        public long getId() { return id; }
        public String getType() { return type; }
        public double getAmount() { return amount; }
        public boolean isValid() { return valid; }
        public long getProcessedTimestamp() { return processedTimestamp; }
        public void setProcessedTimestamp(long processedTimestamp) { 
            this.processedTimestamp = processedTimestamp; 
        }
        public boolean isProcessed() { return processed; }
        public void setProcessed(boolean processed) { 
            this.processed = processed; 
        }
    }
}