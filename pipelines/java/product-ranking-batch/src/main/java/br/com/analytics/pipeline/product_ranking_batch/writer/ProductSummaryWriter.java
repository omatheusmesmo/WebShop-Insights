package br.com.analytics.pipeline.product_ranking_batch.writer;

import br.com.analytics.pipeline.product_ranking_batch.model.ProductDailySummary;
import br.com.analytics.pipeline.product_ranking_batch.processor.ProductAggregationProcessor;
import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.batch.infrastructure.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.infrastructure.item.database.JdbcBatchItemWriter;
import org.springframework.batch.infrastructure.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.beans.factory.annotation.Qualifier;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.List;

public class ProductSummaryWriter implements ItemWriter<ProductDailySummary> {

    private final ProductAggregationProcessor processor;
    private final JdbcBatchItemWriter<ProductDailySummary> delegateWriter;

    public ProductSummaryWriter(
            ProductAggregationProcessor processor,
            @Qualifier("batchDataSource")DataSource batchDataSource) {
        this.processor = processor;
        this.delegateWriter = createDelegateWriter(batchDataSource);
    }

    private JdbcBatchItemWriter<ProductDailySummary> createDelegateWriter(DataSource dataSource){

        final String SQL_UPSERT =
                "INSERT INTO product_performance (product_id, aggregation_day, total_units_sold, total_revenue, " +
                        "revenue_rank, sales_velocity_score, calculation_date) " +
                        "VALUES (:productId, :aggregationDay, :totalUnitsSold, :totalRevenue, :revenueRank, :salesVelocityScore, :calculationDate) " +
                        "ON CONFLICT (product_id, aggregation_day) DO UPDATE SET " +
                        "total_units_sold = EXCLUDED.total_units_sold, " +
                        "total_revenue = EXCLUDED.total_revenue, " +
                        "calculation_date = EXCLUDED.calculation_date";

        return new JdbcBatchItemWriterBuilder<ProductDailySummary>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql(SQL_UPSERT)
                .dataSource(dataSource)
                .build();
    }

    @Override
    public void write(Chunk<? extends ProductDailySummary> chunk) throws Exception {
        Collection<ProductDailySummary> finalSummaries = processor.getAggregatedData().values();

        if(!finalSummaries.isEmpty()){
            System.out.println("INFO: Starting batch of " + finalSummaries.size() + " product resumes.");

            java.util.List<ProductDailySummary> summariesList = new java.util.ArrayList<>(finalSummaries);

            delegateWriter.write(new Chunk<>(summariesList));

        } else {
            System.out.println("INFO: No new aggregation to write to the database.");
        }

    }
}
