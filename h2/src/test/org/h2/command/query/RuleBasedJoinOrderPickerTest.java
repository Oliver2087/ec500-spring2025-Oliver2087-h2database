package org.h2.command.query;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.h2.engine.Database;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.condition.Comparison;
import org.h2.expression.condition.ConditionAndOr;
import org.h2.expression.condition.ConditionAndOrN;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;

public class RuleBasedJoinOrderPickerTest {
    SessionLocal mockSession;
    Database mockDatabase;

    RuleBasedJoinOrderPicker ruleBasedJoinOrderPicker;

    Table customersTable;
    Table ordersTable;
    Table orderDetailsTable;
    Table productsTable;
    Table suppliersTable;
    Table orderPaymentsTable;

    ExpressionColumn customersCustomerId;

    ExpressionColumn ordersCustomerId;
    ExpressionColumn ordersOrderId;

    ExpressionColumn orderDetailsOrderId;
    ExpressionColumn orderDetailsProductId;

    ExpressionColumn productsProductId;
    ExpressionColumn productsSupplierId;

    ExpressionColumn suppliersSupplierId;

    ExpressionColumn orderPaymentsOrderId;

    @BeforeEach
    public void setUp(){
        mockSession = Mockito.mock(SessionLocal.class);
        Mockito.when(mockSession.nextObjectId()).thenReturn(1);

        mockDatabase = Mockito.mock(Database.class);

        // for the purposes of this unit test, we will use four mock tables with
        // multiple relationships between them
        mockSession = Mockito.mock(SessionLocal.class);

        customersTable = Mockito.mock(Table.class);
        customersCustomerId = Mockito.mock(ExpressionColumn.class);
        Mockito.when(customersTable.getName()).thenReturn("customers");
        Mockito.when(customersTable.getRowCountApproximation(mockSession)).thenReturn(10L);
        Mockito.when(customersCustomerId.getTableName()).thenReturn("customers");

        suppliersTable = Mockito.mock(Table.class);
        suppliersSupplierId = Mockito.mock(ExpressionColumn.class);
        Mockito.when(suppliersTable.getName()).thenReturn("suppliers");
        Mockito.when(suppliersTable.getRowCountApproximation(mockSession)).thenReturn(15L);
        Mockito.when(suppliersSupplierId.getTableName()).thenReturn("suppliers");

        productsTable = Mockito.mock(Table.class);
        productsProductId = Mockito.mock(ExpressionColumn.class);
        productsSupplierId = Mockito.mock(ExpressionColumn.class);
        Mockito.when(productsTable.getName()).thenReturn("products");
        Mockito.when(productsTable.getRowCountApproximation(mockSession)).thenReturn(50L);
        Mockito.when(productsSupplierId.getTableName()).thenReturn("products");
        Mockito.when(productsProductId.getTableName()).thenReturn("products");

        orderPaymentsTable = Mockito.mock(Table.class);
        orderPaymentsOrderId = Mockito.mock(ExpressionColumn.class);
        Mockito.when(orderPaymentsTable.getName()).thenReturn("order_payments");
        Mockito.when(orderPaymentsTable.getRowCountApproximation(mockSession)).thenReturn(150L);
        Mockito.when(orderPaymentsOrderId.getTableName()).thenReturn("order_payments");

        ordersTable = Mockito.mock(Table.class);
        ordersCustomerId = Mockito.mock(ExpressionColumn.class);
        ordersOrderId = Mockito.mock(ExpressionColumn.class);
        Mockito.when(ordersTable.getName()).thenReturn("orders");
        Mockito.when(ordersTable.getRowCountApproximation(mockSession)).thenReturn(200L);
        Mockito.when(ordersCustomerId.getTableName()).thenReturn("orders");
        Mockito.when(ordersOrderId.getTableName()).thenReturn("orders");

        orderDetailsTable = Mockito.mock(Table.class);
        orderDetailsOrderId = Mockito.mock(ExpressionColumn.class);
        orderDetailsProductId = Mockito.mock(ExpressionColumn.class);
        Mockito.when(orderDetailsTable.getName()).thenReturn("order_details");
        Mockito.when(orderDetailsTable.getRowCountApproximation(mockSession)).thenReturn(500L);
        Mockito.when(orderDetailsOrderId.getTableName()).thenReturn("order_details");
        Mockito.when(orderDetailsProductId.getTableName()).thenReturn("order_details");

    }

    @Test
    public void bestOrder_singleTable(){
        TableFilter tableFilter = new TableFilter(mockSession, customersTable, "customers", true, null, 0, null);
        tableFilter.setFullCondition(null);

        List<TableFilter> expectedFilters = List.of(tableFilter);
        TableFilter[] inputFilters = {tableFilter};

        ruleBasedJoinOrderPicker = new RuleBasedJoinOrderPicker(mockSession, inputFilters);
        List<TableFilter> result = Arrays.asList(ruleBasedJoinOrderPicker.bestOrder());

        assertEquals(expectedFilters, result);
    }

    @Test
    public void bestOrder_twoTablesSingleJoin(){
        // Create the join condition: customers.customer_id = orders.customer_id
        Expression ordersAndCustomer = new Comparison(
                Comparison.EQUAL,
                customersCustomerId,
                ordersCustomerId,
                false);

        Expression fullCondition = new ConditionAndOrN(ConditionAndOr.AND,
                List.of(
                        ordersAndCustomer
                )
        );

        // Create the table filter for orders.
        TableFilter ordersFilter = new TableFilter(mockSession, ordersTable, "orders", true, null, 0, null);
        ordersFilter.setFullCondition(fullCondition);

        // Create the table filter for customers.
        TableFilter customersFilter = new TableFilter(mockSession, customersTable, "customers", true, null, 0, null);
        customersFilter.setFullCondition(fullCondition);

        // Expect the best join order to be: customers first (because its table is smaller) then orders.
        List<TableFilter> expectedFilters = List.of(customersFilter, ordersFilter);
        TableFilter[] inputFilters = { customersFilter, ordersFilter };

        ruleBasedJoinOrderPicker = new RuleBasedJoinOrderPicker(mockSession, inputFilters);
        List<TableFilter> result = Arrays.asList(ruleBasedJoinOrderPicker.bestOrder());

        assertEquals(expectedFilters, result);
    }

    @Test
    public void bestOrder_threeTablesMultipleJoins(){
        Expression fullCondition = new ConditionAndOrN(ConditionAndOr.AND,
                List.of(
                        new Comparison(Comparison.EQUAL, orderDetailsProductId, productsProductId, false),
                        new Comparison(Comparison.EQUAL, orderDetailsOrderId, ordersOrderId, false)
                )
        );

        TableFilter productsFilter = new TableFilter(mockSession, productsTable, "products", true, null, 0, null);
        TableFilter orderDetailsFilter = new TableFilter(mockSession, orderDetailsTable, "order_details", true, null, 0, null);
        TableFilter ordersFilter = new TableFilter(mockSession, ordersTable, "orders", true, null, 0, null);

        productsFilter.setFullCondition(fullCondition);
        orderDetailsFilter.setFullCondition(fullCondition);
        ordersFilter.setFullCondition(fullCondition);

        // size order is products, order details, orders
        List<TableFilter> expectedFilters = List.of(productsFilter, orderDetailsFilter, ordersFilter);

        TableFilter[] inputFilters = {orderDetailsFilter, productsFilter, ordersFilter};

        ruleBasedJoinOrderPicker = new RuleBasedJoinOrderPicker(mockSession, inputFilters);
        List<TableFilter> result = Arrays.asList(ruleBasedJoinOrderPicker.bestOrder());

        assertEquals(expectedFilters, result);
    }

    @Test
    public void bestOrder_fourTablesMultipleJoins(){
        Expression fullCondition = new ConditionAndOrN(ConditionAndOr.AND,
                List.of(
                        new Comparison(Comparison.EQUAL, customersCustomerId, ordersCustomerId, false),
                        new Comparison(Comparison.EQUAL, ordersCustomerId, orderDetailsOrderId, false),
                        new Comparison(Comparison.EQUAL, orderDetailsProductId, productsProductId, false)
                )
        );

        TableFilter customersFilter = new TableFilter(mockSession, customersTable, "customers", true, null, 0, null);
        TableFilter ordersFilter = new TableFilter(mockSession, ordersTable, "orders", true, null, 0, null);
        TableFilter productsFilter = new TableFilter(mockSession, productsTable, "products", true, null, 0, null);
        TableFilter orderDetailsFilter = new TableFilter(mockSession, orderDetailsTable, "order_details", true, null, 0, null);

        customersFilter.setFullCondition(fullCondition);
        ordersFilter.setFullCondition(fullCondition);
        productsFilter.setFullCondition(fullCondition);
        orderDetailsFilter.setFullCondition(fullCondition);

        // size order is customers, orders, order_details, products
        List<TableFilter> expectedFilters = List.of(customersFilter, ordersFilter, orderDetailsFilter, productsFilter);

        TableFilter[] inputFilters = {customersFilter, ordersFilter, orderDetailsFilter, productsFilter};

        ruleBasedJoinOrderPicker = new RuleBasedJoinOrderPicker(mockSession, inputFilters);
        List<TableFilter> result = Arrays.asList(ruleBasedJoinOrderPicker.bestOrder());

        assertEquals(expectedFilters, result);
    }

    @Test
    public void bestOrder_fiveTablesMultipleJoins(){

        Expression fullCondition = new ConditionAndOrN(ConditionAndOr.AND,
                List.of(
                        new Comparison(Comparison.EQUAL, orderDetailsProductId, productsProductId, false),
                        new Comparison(Comparison.EQUAL, productsSupplierId, suppliersSupplierId, false),
                        new Comparison(Comparison.EQUAL, orderDetailsOrderId, ordersOrderId, false),
                        new Comparison(Comparison.EQUAL, ordersCustomerId, customersCustomerId, false)
                )
        );

        TableFilter customersFilter = new TableFilter(mockSession, customersTable, "customers", true, null, 0, null);
        TableFilter ordersFilter = new TableFilter(mockSession, ordersTable, "orders", true, null, 0, null);
        TableFilter productsFilter = new TableFilter(mockSession, productsTable, "products", true, null, 0, null);
        TableFilter orderDetailsFilter = new TableFilter(mockSession, orderDetailsTable, "order_details", true, null, 0, null);
        TableFilter suppliersFilter = new TableFilter(mockSession, suppliersTable, "suppliers", true, null, 0, null);

        customersFilter.setFullCondition(fullCondition);
        ordersFilter.setFullCondition(fullCondition);
        orderDetailsFilter.setFullCondition(fullCondition);
        productsFilter.setFullCondition(fullCondition);
        suppliersFilter.setFullCondition(fullCondition);

        // size order is customers, orders, order_details, products, suppliers
        List<TableFilter> expectedFilters = List.of(customersFilter, ordersFilter, orderDetailsFilter,productsFilter, suppliersFilter);

        TableFilter[] inputFilters = {orderDetailsFilter, productsFilter, suppliersFilter, ordersFilter, customersFilter};

        ruleBasedJoinOrderPicker = new RuleBasedJoinOrderPicker(mockSession, inputFilters);
        List<TableFilter> result = Arrays.asList(ruleBasedJoinOrderPicker.bestOrder());

        assertEquals(expectedFilters, result);
    }

    @Test
    public void bestOrder_fourTablesMoreOptionsMultipleJoins(){
        Expression fullCondition = new ConditionAndOrN(ConditionAndOr.AND,
                List.of(
                        new Comparison(Comparison.EQUAL, orderDetailsOrderId, orderPaymentsOrderId, false),
                        new Comparison(Comparison.EQUAL, ordersOrderId, orderDetailsOrderId, false),
                        new Comparison(Comparison.EQUAL, orderPaymentsOrderId, ordersOrderId, false),
                        new Comparison(Comparison.EQUAL, orderDetailsProductId, productsProductId, false)
                )
        );

        TableFilter orderDetailsFilter = new TableFilter(mockSession, orderDetailsTable, "order_details", true, null, 0, null);
        TableFilter orderPaymentsFilter = new TableFilter(mockSession, orderPaymentsTable, "order_payments", true, null, 0, null);
        TableFilter ordersFilter = new TableFilter(mockSession, ordersTable, "orders", true, null, 0, null);
        TableFilter productsFilter = new TableFilter(mockSession, productsTable, "products", true, null, 0, null);

        orderDetailsFilter.setFullCondition(fullCondition);
        orderPaymentsFilter.setFullCondition(fullCondition);
        ordersFilter.setFullCondition(fullCondition);
        productsFilter.setFullCondition(fullCondition);

        // size order is products, order_details, order_payments, orders
        List<TableFilter> expectedFilters = List.of(productsFilter, orderDetailsFilter, orderPaymentsFilter, ordersFilter);

        TableFilter[] inputFilters = {orderDetailsFilter, orderPaymentsFilter, ordersFilter, productsFilter};

        ruleBasedJoinOrderPicker = new RuleBasedJoinOrderPicker(mockSession, inputFilters);
        List<TableFilter> result = Arrays.asList(ruleBasedJoinOrderPicker.bestOrder());

        assertEquals(expectedFilters, result);
    }
}