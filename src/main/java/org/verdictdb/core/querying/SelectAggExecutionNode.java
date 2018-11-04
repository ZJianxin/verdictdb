package org.verdictdb.core.querying;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.connection.InMemoryAggregate;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.execplan.ExecutionTokenQueue;
import org.verdictdb.core.sqlobject.CreateTableAsSelectQuery;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.exception.VerdictDBException;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class SelectAggExecutionNode extends AggExecutionNode {

  private static final long serialVersionUID = 47947858649322912L;

  private static long selectAggID = 0;

  private static final String inMemoryTableName = "VERDICTDB_SELECTAGG";

  public SelectAggExecutionNode(IdCreator idCreator, SelectQuery selectQuery) {
    super(idCreator, selectQuery);
  }

  public static SelectAggExecutionNode create(AggExecutionNode node) {
    SelectAggExecutionNode selectAggExecutionNode = new SelectAggExecutionNode(node.namer, node.selectQuery);
    selectAggExecutionNode.aggMeta = node.aggMeta;
    selectAggExecutionNode.placeholderRecords = node.placeholderRecords;
    selectAggExecutionNode.placeholderTablesinFilter = node.placeholderTablesinFilter;
    for (Pair<ExecutableNodeBase, Integer> pair:node.sources) {
      ExecutableNodeBase child = pair.getLeft();
      selectAggExecutionNode.subscribeTo(child, pair.getRight());
      node.cancelSubscriptionTo(child);
    }
    return selectAggExecutionNode;
  }

  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    CreateTableAsSelectQuery query = (CreateTableAsSelectQuery)super.createQuery(tokens);
    return query.getSelect();
  }

  @Override
  public ExecutionInfoToken createToken(DbmsQueryResult result) {
    ExecutionInfoToken token = new ExecutionInfoToken();
    token.setKeyValue("aggMeta", aggMeta);
    token.setKeyValue("dependentQuery", this.selectQuery);

    // insert value to in memory database.
    String tableName;
    synchronized (SelectAggExecutionNode.class) {
      tableName = inMemoryTableName+selectAggID;
      selectAggID++;
    }
    try {
      InMemoryAggregate.createTable(result, tableName);
    } catch (SQLException e) {
      e.printStackTrace();
    }
    token.setKeyValue("schemaName", "PUBLIC");
    token.setKeyValue("tableName", tableName);
    return token;
  }

}