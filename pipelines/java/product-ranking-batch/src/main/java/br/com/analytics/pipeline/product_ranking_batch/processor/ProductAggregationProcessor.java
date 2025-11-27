package br.com.analytics.pipeline.product_ranking_batch.processor;

import br.com.analytics.pipeline.product_ranking_batch.model.AggregationKey;
import br.com.analytics.pipeline.product_ranking_batch.model.OrderItem;
import br.com.analytics.pipeline.product_ranking_batch.model.ProductDailySummary;
import org.jspecify.annotations.Nullable;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.repository.persistence.StepExecution;
import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.ItemProcessor;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ProductAggregationProcessor implements ItemProcessor<OrderItem, ProductDailySummary> {

    private final Map<AggregationKey, ProductDailySummary> aggregatedData = new HashMap<>();
    private final Set<Long> processedOrdersForShipping = new HashSet<>();

    private Chunk<ProductDailySummary> finalAggregates;

    public Map<AggregationKey, ProductDailySummary> getAggregatedData() {
        return aggregatedData;
    }

    @Override
    public ProductDailySummary process(OrderItem item) throws Exception {

        if(!"Completed".equals(item.orderStatus())){
            return null;
        }

        AggregationKey key = new AggregationKey(item.productId(), item.orderDate());

        BigDecimal itemRevenue = item.unitPriceAtSale().multiply(BigDecimal.valueOf(item.quantity()));

        ProductDailySummary summary = aggregatedData.getOrDefault(key, createNewSummary(item));

        summary = updateSummary(summary, item.quantity(), itemRevenue);

        if(!processedOrdersForShipping.contains(item.orderId())){
            summary = addShippingCostOnce(summary, item.shippingCost());
            processedOrdersForShipping.add(item.orderId());
        }

        aggregatedData.put(key, summary);
        return null;
    }

    private ProductDailySummary createNewSummary(OrderItem item) {
        return new ProductDailySummary(
                item.productId(),
                item.orderDate(),
                0L,
                BigDecimal.ZERO,
                null,
                null,
                LocalDateTime.now()
        );
    }

    private ProductDailySummary updateSummary(ProductDailySummary current, Integer units, BigDecimal revenue) {
        return new ProductDailySummary(
                current.productId(),
                current.aggregationDay(),
                current.totalUnitsSold() + units,
                current.totalRevenue().add(revenue),
                current.revenueRank(),
                current.salesVelocityScore(),
                current.calculationDate()
        );
    }

    private ProductDailySummary addShippingCostOnce(ProductDailySummary current, BigDecimal shippingCost) {
        return new ProductDailySummary(
                current.productId(),
                current.aggregationDay(),
                current.totalUnitsSold(),
                current.totalRevenue().add(shippingCost),
                current.revenueRank(),
                current.salesVelocityScore(),
                current.calculationDate()
        );
    }

    @AfterStep
    public void afterStep(StepExecution stepExecution) {
        System.out.println("INFO: In-memory aggregation complete. " + aggregatedData.size() + " summaries created.");
    }
}
