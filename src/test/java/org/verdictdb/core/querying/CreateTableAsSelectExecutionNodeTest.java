package org.verdictdb.core.querying;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.core.execplan.ExecutableNodeRunner;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.sqlobject.AsteriskColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlsyntax.H2Syntax;

public class CreateTableAsSelectExecutionNodeTest {
  
  static String originalSchema = "originalschema";

  static String originalTable = "originalschema";

  static String newSchema = "newschema";

  static String newTable  = "newtable";

  static int aggblockCount = 2;

  static DbmsConnection conn;

  @BeforeClass
  public static void setupDbConnAndScrambledTable() throws SQLException, VerdictDBException {
    final String DB_CONNECTION = "jdbc:h2:mem:createasselecttest;DB_CLOSE_DELAY=-1";
    final String DB_USER = "";
    final String DB_PASSWORD = "";
    conn = new JdbcConnection(DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD), new H2Syntax());
    conn.execute(String.format("CREATE SCHEMA \"%s\"", originalSchema));
    conn.execute(String.format("CREATE SCHEMA \"%s\"", newSchema));
    populateData(conn, originalSchema, originalTable);
  }

  @Test
  public void testExecuteNode() throws VerdictDBException {
    BaseTable base = new BaseTable(originalSchema, originalTable, "t");
    SelectQuery query = SelectQuery.create(Arrays.<SelectItem>asList(new AsteriskColumn()), base);
    ExecutableNodeBase root = CreateTableAsSelectNode.create(new QueryExecutionPlan("newschema"), query);
//    ExecutionInfoToken token = new ExecutionInfoToken();
    ExecutionInfoToken newTableName = 
        ExecutableNodeRunner.execute(conn, root);
    
    String schemaName = (String) newTableName.getValue("schemaName");
    String tableName = (String) newTableName.getValue("tableName");
    conn.execute(String.format("DROP TABLE \"%s\".\"%s\"", schemaName, tableName));
  }

  static void populateData(DbmsConnection conn, String schemaName, String tableName) throws VerdictDBDbmsException {
    conn.execute(String.format("CREATE TABLE \"%s\".\"%s\"(\"id\" int, \"value\" double)", schemaName, tableName));
    for (int i = 0; i < 2; i++) {
      conn.execute(String.format("INSERT INTO \"%s\".\"%s\"(\"id\", \"value\") VALUES(%s, %f)",
          schemaName, tableName, i, (double) i+1));
    }
  }
  
}