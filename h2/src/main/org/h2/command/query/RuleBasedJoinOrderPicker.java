package org.h2.command.query;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.condition.Comparison;
import org.h2.table.TableFilter;

import java.util.*;


public class RuleBasedJoinOrderPicker {
    final SessionLocal session;
    final TableFilter[] filters;

    public RuleBasedJoinOrderPicker(SessionLocal session, TableFilter[] filters) {
        this.session = session;
        this.filters = filters;
    }

    public TableFilter[] bestOrder() {
        List<TableFilter> remaining = new ArrayList<>(Arrays.asList(filters));
        List<TableFilter> ordered = new ArrayList<>();

        // Pick the first table with the fewest rows
        TableFilter first = Collections.min(remaining, Comparator.comparingLong(this::estimateRows));
        ordered.add(first);
        remaining.remove(first);

        // Keep adding connected tables with the fewest rows
        while (!remaining.isEmpty()) {
            TableFilter next = selectNextConnectedTable(ordered, remaining);
            if (next == null) {
                throw new IllegalStateException("No remaining table is connected; refusing to create cartesian product.");
            }
            ordered.add(next);
            remaining.remove(next);
        }

        return ordered.toArray(new TableFilter[0]);
    }

    private TableFilter selectNextConnectedTable(List<TableFilter> ordered, List<TableFilter> remaining) {
        Set<String> alreadyChosenTables = new HashSet<>();
        for (TableFilter tf : ordered) {
            alreadyChosenTables.add(tf.getTable().getName());
        }
        TableFilter best = null;
        long bestRowCount = Long.MAX_VALUE;
        for (TableFilter candidate : remaining) {
            if (isConnected(candidate, alreadyChosenTables)) {
                long rowCount = estimateRows(candidate);
                if (rowCount < bestRowCount) {
                    best = candidate;
                    bestRowCount = rowCount;
                }
            }
        }
        return best;
    }

    private boolean isConnected(TableFilter candidate, Set<String> alreadyChosenTables) {
        Expression condition = candidate.getFullCondition();
        if (condition == null) return false;

        String candidateTableName = candidate.getTable().getName();
        for (String chosen : alreadyChosenTables) {
            if (isDirectlyConnected(condition, candidateTableName, chosen)) {
                return true;
            }
        }
        return false;
    }

    private boolean isDirectlyConnected(Expression expr, String candidateTableName, String chosenTableName) {
        if (expr instanceof Comparison) {
            Comparison cmp = (Comparison) expr;
            if (cmp.getCompareType() == Comparison.EQUAL) {
                // Get the tables referenced by each side of the comparison
                Set<String> leftTables = extractReferencedTables(cmp.getSubexpression(0));
                Set<String> rightTables = extractReferencedTables(cmp.getSubexpression(1));

                // Check if one side is from the candidate table and the other from the chosen table.
                if ((leftTables.contains(candidateTableName) && rightTables.contains(chosenTableName))
                        || (leftTables.contains(chosenTableName) && rightTables.contains(candidateTableName))) {
                    return true;
                }
            }
        }
        // Recursively check all subexpressions.
        for (int i = 0; i < expr.getSubexpressionCount(); i++) {
            if (isDirectlyConnected(expr.getSubexpression(i), candidateTableName, chosenTableName)) {
                return true;
            }
        }
        return false;
    }

    private Set<String> extractReferencedTables(Expression expr) {
        Set<String> result = new HashSet<>();
        if (expr == null) return result;

        if (expr instanceof ExpressionColumn) {
            ExpressionColumn col = (ExpressionColumn) expr;
            if (col.getTableName() != null) {
                result.add(col.getTableName());
            }
        }
        for (int i = 0; i < expr.getSubexpressionCount(); i++) {
            result.addAll(extractReferencedTables(expr.getSubexpression(i)));
        }
        return result;
    }

    private long estimateRows(TableFilter filter) {
        try {
            return filter.getTable().getRowCountApproximation(session);
        } catch (Exception e) {
            return Long.MAX_VALUE;
        }
    }
}