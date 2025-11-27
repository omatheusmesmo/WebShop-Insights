package br.com.analytics.pipeline.product_ranking_batch.model;

import java.math.BigDecimal;
import java.time.LocalDate;

public record OrderItem(
        Long orderId,
        Long productId,
        LocalDate orderDate,
        Integer quantity,
        BigDecimal unitPriceAtSale,
        BigDecimal shippingCost,
        String orderStatus
) {
}
