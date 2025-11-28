package br.com.analytics.pipeline.product_ranking_batch.tasklet;

import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

public class RankingCalculationTasklet implements Tasklet {

    private final JdbcTemplate jdbcTemplate;

    public RankingCalculationTasklet(@Qualifier("batchDataSource") DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception{
        final String SQL_UPDATE_METRICS =
                "WITH CalculatedMetrics AS (" +
                        "    SELECT " +
                        "        product_id, " +
                        "        aggregation_day, " +
                        "        " +
                        "        -- 1. Calculate Revenue Rank (partitioned by day) " +
                        "        RANK() OVER (PARTITION BY aggregation_day ORDER BY total_revenue DESC) as calculated_rank, " +
                        "        " +
                        "        -- 2. Calculate Sales Velocity Score (7-day average revenue, excluding current day) " +
                        "        AVG(total_revenue) OVER (" +
                        "            PARTITION BY product_id " +
                        "            ORDER BY aggregation_day ASC " +
                        "            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING" +
                        "        ) as calculated_velocity " +
                        "    FROM product_performance " +
                        ") " +
                        "UPDATE product_performance pp " +
                        "SET " +
                        "    revenue_rank = cm.calculated_rank, " +
                        "    sales_velocity_score = cm.calculated_velocity " +
                        "FROM CalculatedMetrics cm " +
                        "WHERE pp.product_id = cm.product_id AND pp.aggregation_day = cm.aggregation_day";

        int updatedRows = jdbcTemplate.update(SQL_UPDATE_METRICS);

        System.out.println("INFO: Ranking and Sales Velocity calculated and updated for " + updatedRows + " rows.");

        return RepeatStatus.FINISHED;
    }
}
