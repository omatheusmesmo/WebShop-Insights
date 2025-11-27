package br.com.analytics.pipeline.product_ranking_batch.reader;

import br.com.analytics.pipeline.product_ranking_batch.model.OrderItem;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;

public class OrderItemRowMapper implements RowMapper<OrderItem> {

    @Override
    public OrderItem mapRow(ResultSet resultSet, int rowNum) throws SQLException{
        return new OrderItem(
                resultSet.getLong("orderId"),
                resultSet.getLong("productId"),
                resultSet.getObject("orderDate", LocalDate.class),
                resultSet.getInt("quantity"),
                resultSet.getBigDecimal("unitPriceAtSale"),
                resultSet.getBigDecimal("shippingCost"),
                resultSet.getString("orderStatus")
        );
    }
}
