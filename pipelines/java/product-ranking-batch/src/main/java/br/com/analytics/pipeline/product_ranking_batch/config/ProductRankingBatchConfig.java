package br.com.analytics.pipeline.product_ranking_batch.config;

import br.com.analytics.pipeline.product_ranking_batch.model.OrderItem;
import br.com.analytics.pipeline.product_ranking_batch.model.ProductDailySummary;
import br.com.analytics.pipeline.product_ranking_batch.processor.ProductAggregationProcessor;
import br.com.analytics.pipeline.product_ranking_batch.reader.OrderItemRowMapper;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.parameters.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.batch.infrastructure.item.ItemReader;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.batch.infrastructure.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.time.LocalDate;

@Configuration
@EnableBatchProcessing
public class ProductRankingBatchConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;

    public ProductRankingBatchConfig(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
    }

    private static final String ORDER_ITEMS_QUERY =
                    "SELECT " +
                    "od.order_id AS orderId, od.product_id AS productId, CAST(o.order_date AS DATE) AS orderDate, " +
                    "od.quantity AS quantity, od.unit_price_at_sale AS unitPriceAtSale, " +
                    "o.shipping_cost AS shippingCost, o.order_status AS orderStatus " +
                    "FROM order_details od " +
                    "JOIN orders o ON o.order_id = od.order_id " +
                    "WHERE o.order_date > ? " +
                    "AND o.order_status IN ('Completed', 'Shipped') " +
                    "ORDER BY orderDate, orderId";

    @Bean
    public ItemReader<OrderItem> itemReader(
            @Qualifier("appDataSource") DataSource appDataSource
            ){
        LocalDate latestDate = LocalDate.parse("1900-01-01");

        return new JdbcCursorItemReaderBuilder<OrderItem>()
                .name("orderItemReader")
                .dataSource(appDataSource)
                .sql(ORDER_ITEMS_QUERY)
                .rowMapper(new OrderItemRowMapper())
                .fetchSize(1000)
                .queryArguments(latestDate)
                .build();
    }

    @Bean
    public ItemProcessor<OrderItem, ProductDailySummary> itemProcessor(){
        return new ProductAggregationProcessor();
    }

    @Bean
    public ItemWriter<ProductDailySummary> itemWriter(){
        return items ->{
            System.out.println("INFO: Writing "+ items.size() + " product daily summaries ...");
        };
    }

    @Bean
    public Step aggregationStep(
            ItemReader<OrderItem> reader,
            ItemProcessor<OrderItem, ProductDailySummary> processor,
            ItemWriter<ProductDailySummary> writer
    ){
        return new StepBuilder("aggregationStep", jobRepository)
                .<OrderItem, ProductDailySummary>chunk(500)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    @Bean
    public Job productRankingDailyJob(Step aggregationStep){
        return new JobBuilder("productRankingDailyJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(aggregationStep)
                .build();
    }

}
