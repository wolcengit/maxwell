package com.zendesk.maxwell.producer;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.visitor.VisitorFeature;
import com.alibaba.druid.util.JdbcConstants;
import com.alibaba.druid.util.JdbcUtils;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;

public class SyncMysqlProducer extends AbstractProducer {
	private static final Logger LOGGER = LoggerFactory.getLogger(SyncMysqlProducer.class);

	private final Collection<RowMap> txRows = new ArrayList<>();

	public SyncMysqlProducer(MaxwellContext context) {
		super(context);
	}

	@Override
	public void push(RowMap r) throws Exception {
		// bootstrap-start  bootstrap-insert  bootstrap-complete
		if ("bootstrap-insert".equals(r.getRowType())) {
			execute(buildBootstrapInsert(r));
			context.setPosition(r.getPosition());
			return;
		}

		// insert delete update
		if ("insert,delete,update".contains(r.getRowType())) {
			if (StringUtils.isNotBlank(r.getRowQuery())) {
				// store uncommitted row in buffer
				txRows.add(r);
			}
			if (r.isTXCommit()) {
				// This row is the final and closing row of a transaction. Stream all rows of buffered transaction to stdout
				execute(txRows);
				txRows.clear();
				// Only now, after finally having "persisted" all buffered rows to stdout is it safe to
				// store the producers position.
				context.setPosition(r.getPosition());
			}
		}
	}

	private String buildBootstrapInsert(RowMap r) {
		MySqlInsertStatement sqlInsertStatement = new MySqlInsertStatement();
		sqlInsertStatement.setTableName(new SQLIdentifierExpr(r.getTable()));
		SQLInsertStatement.ValuesClause valuesClause = new SQLInsertStatement.ValuesClause();
		sqlInsertStatement.addValueCause(valuesClause);
		LinkedHashMap<String, Object> data = r.getData();
		for (String k : data.keySet()) {
			Object v = data.get(k);
			sqlInsertStatement.addColumn(new SQLIdentifierExpr(k));
			valuesClause.addValue(v);
		}
		String sql = SQLUtils.toSQLString(sqlInsertStatement, JdbcConstants.MYSQL, new SQLUtils.FormatOption(VisitorFeature.OutputNameQuote));
		LOGGER.debug("BootstrapInsert >> {}", sql);
		return sql;
	}

	private void execute(String sql) {
		try (Connection connection = this.context.getSyncMysqlConnection()) {
			connection.setAutoCommit(true);
			JdbcUtils.execute(connection, sql);
		} catch (SQLException e) {
			LOGGER.error("error execute : {} ", sql, e);
		}
	}

	private void execute(Collection<RowMap> rs) {
		try (Connection connection = this.context.getSyncMysqlConnection()) {
			connection.setAutoCommit(false);
			try {
				for (RowMap r : rs) {
					String sql = r.getRowQuery();
					JdbcUtils.execute(connection, sql);
				}
				connection.commit();
			} catch (SQLException e) {
				connection.rollback();
				LOGGER.error("error execute ", e);
			}
		} catch (SQLException e) {
			LOGGER.error("error execute ", e);
		}
	}
}
