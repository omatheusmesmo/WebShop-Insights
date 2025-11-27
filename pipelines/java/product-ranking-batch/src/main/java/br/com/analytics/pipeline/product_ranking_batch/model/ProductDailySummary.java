package br.com.analytics.pipeline.product_ranking_batch.model;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

public record ProductDailySummary(
        Long productId,
        LocalDate aggregationDay,
        Long totalUnitsSold,
        BigDecimal totalRevenue,
        Integer revenueRank,
        BigDecimal salesVelocityScore,
        LocalDateTime calculationDate
) {
}
