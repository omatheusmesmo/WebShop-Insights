package br.com.analytics.pipeline.product_ranking_batch.tasklet;

import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.dao.EmptyResultDataAccessException;

import javax.sql.DataSource;
import java.time.LocalDate;

public class RankingCalculationTasklet implements Tasklet {

    private final JdbcTemplate jdbcTemplate;

    private static final String SQL_MIN_UNPROCESSED_DAY =
            "SELECT MIN(aggregation_day) FROM product_performance WHERE revenue_rank IS NULL";

    public RankingCalculationTasklet(@Qualifier("batchDataSource") DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

        LocalDate minUnprocessedDay;
        try {
            minUnprocessedDay = jdbcTemplate.queryForObject(SQL_MIN_UNPROCESSED_DAY, LocalDate.class);
        } catch (EmptyResultDataAccessException e) {
            minUnprocessedDay = null;
        }

        if (minUnprocessedDay == null) {
            System.out.println("INFO: All aggregation days already have revenue ranking.");
            return RepeatStatus.FINISHED;
        }

        LocalDate hwmFilterStartDate = minUnprocessedDay.minusDays(7);


        System.out.println("INFO: Starting ranking calculation from " + minUnprocessedDay + ". CTE filter starts at " + hwmFilterStartDate);

        final String SQL_UPDATE_METRICS_HWM =
                "WITH CalculatedMetrics AS (" +
                        "    SELECT " +
                        "        product_id, " +
                        "        aggregation_day, " +
                        "        RANK() OVER (PARTITION BY aggregation_day ORDER BY total_revenue DESC) as calculated_rank, " +
                        "        AVG(total_revenue) OVER (" +
                        "            PARTITION BY product_id " +
                        "            ORDER BY aggregation_day ASC " +
                        "            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING" +
                        "        ) as calculated_velocity " +
                        "    FROM product_performance pp_inner " +
                        "    WHERE pp_inner.aggregation_day >= ?" + // Filter the CTE to reduce scan volume
                        ") " +
                        "UPDATE product_performance pp " +
                        "SET " +
                        "    revenue_rank = cm.calculated_rank, " +
                        "    sales_velocity_score = cm.calculated_velocity " +
                        "FROM CalculatedMetrics cm " +
                        "WHERE pp.product_id = cm.product_id " +
                        "    AND pp.aggregation_day = cm.aggregation_day " +
                        "    AND pp.revenue_rank IS NULL";

        int updatedRows = jdbcTemplate.update(SQL_UPDATE_METRICS_HWM, hwmFilterStartDate);

        System.out.println("INFO: Ranking and Sales Velocity calculated and updated for " + updatedRows + " rows.");

        return RepeatStatus.FINISHED;
    }
}