package br.com.analytics.pipeline.product_ranking_batch.model;

import java.time.LocalDate;

public record AggregationKey(
        Long productId,
        LocalDate orderDate
) {
}
