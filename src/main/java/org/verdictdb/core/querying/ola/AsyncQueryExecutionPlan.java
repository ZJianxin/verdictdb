package org.verdictdb.core.querying.ola;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.verdictdb.core.querying.AggExecutionNode;
import org.verdictdb.core.querying.ExecutableNodeBase;
import org.verdictdb.core.querying.QueryExecutionPlan;
import org.verdictdb.core.querying.QueryNodeBase;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.AliasReference;
import org.verdictdb.core.sqlobject.AliasedColumn;
import org.verdictdb.core.sqlobject.AsteriskColumn;
import org.verdictdb.core.sqlobject.BaseColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.ConstantColumn;
import org.verdictdb.core.sqlobject.JoinTable;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.UnnamedColumn;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBTypeException;
import org.verdictdb.exception.VerdictDBValueException;

public class AsyncQueryExecutionPlan extends QueryExecutionPlan {

  private static final long serialVersionUID = -1670795390245860583L;
  
  private int aggColumnIdentiferNum = 0;
  
  private int verdictdbTierIndentiferNum = 0;

  static final String VERDICTDB_TIER_COLUMN_NAME = "verdictdb_tier_internal";

  private AsyncQueryExecutionPlan(String scratchpadSchemaName, ScrambleMetaSet scrambleMeta)
      throws VerdictDBException {
    super(scratchpadSchemaName, scrambleMeta);
  }

  public static AsyncQueryExecutionPlan create(QueryExecutionPlan plan) throws VerdictDBException {
    if (plan instanceof AsyncQueryExecutionPlan) {
      System.err.println("It is already an asyncronous plan.");
      throw new VerdictDBTypeException(plan);
    }

    AsyncQueryExecutionPlan asyncPlan = 
        new AsyncQueryExecutionPlan(plan.getScratchpadSchemaName(), plan.getScrambleMeta());
    ExecutableNodeBase newRoot = 
        asyncPlan.makeAsyncronousAggIfAvailable(plan.getRootNode());
    asyncPlan.setRootNode(newRoot);
    return asyncPlan;
  }

  /**
   * Returns an asynchronous version of the given plan.
   * 
   * @param root The root execution node of ALL nodes (i.e., not just the top agg node)
   * @return
   * @throws VerdictDBException
   */
  ExecutableNodeBase makeAsyncronousAggIfAvailable(ExecutableNodeBase root) 
      throws VerdictDBException {
    List<AggExecutionNodeBlock> aggBlocks = identifyTopAggBlocks(scrambleMeta, root);

    // converted nodes should be used in place of the original nodes.
    for (int i = 0; i < aggBlocks.size(); i++) {
      // this node block contains the links to those nodes belonging to this block.
      AggExecutionNodeBlock nodeBlock = aggBlocks.get(i);
      SelectQuery originalQuery = null;
      if (nodeBlock.getBlockRootNode() instanceof AggExecutionNode) {
        originalQuery = ((AggExecutionNode) nodeBlock.getBlockRootNode()).getSelectQuery();
      }
      ExecutableNodeBase oldNode = nodeBlock.getBlockRootNode();
//      ExecutableNodeBase newNode = nodeBlock.convertToProgressiveAgg(scrambleMeta);
      ExecutableNodeBase newNode = convertToProgressiveAgg(scrambleMeta, nodeBlock);
      if (newNode instanceof AsyncAggExecutionNode && originalQuery!=null) {
        ((AsyncAggExecutionNode) newNode).setSelectQuery(originalQuery);
      }
      List<ExecutableNodeBase> parents = oldNode.getExecutableNodeBaseParents();
      for (ExecutableNodeBase parent : parents) {
        Integer channel = parent.getChannelForSource(oldNode);
        if (channel == null) {
          // do nothing
        } else {
          parent.cancelSubscriptionTo(oldNode);
          parent.subscribeTo(newNode, channel);
        }
      }
    }

    return root;
  }

  /**
   * Converts the root node and its descendants into the configuration that enables progressive aggregation.
   *
   * Basically aggregate subqueries are blocking operations while others operations are divided into smaller-
   * scale operations (which involve different portions of data).
   *
   * @param nodeBlock
   * @return Returns the root of the multiple aggregation nodes (each of which involves different combinations
   * of partitions)
   * @throws VerdictDBValueException
   */
  public ExecutableNodeBase convertToProgressiveAgg(
      ScrambleMetaSet scrambleMeta, AggExecutionNodeBlock aggNodeBlock)
          throws VerdictDBValueException {
    
    List<ExecutableNodeBase> blockNodes = aggNodeBlock.getNodesInBlock();

    List<ExecutableNodeBase> individualAggNodes = new ArrayList<>();
    List<ExecutableNodeBase> combiners = new ArrayList<>();
    //    ScrambleMeta scrambleMeta = idCreator.getScrambleMeta();

    // First, plan how to perform block aggregation
    // filtering predicates that must inserted into different scrambled tables are identified.
    List<Pair<ExecutableNodeBase, Triple<String, String, String>>> scrambledNodes =
        identifyScrambledNodes(scrambleMeta, blockNodes);
    List<Pair<String, String>> scrambles = new ArrayList<>();
    for (Pair<ExecutableNodeBase, Triple<String, String, String>> a : scrambledNodes) {
      String schemaName = a.getRight().getLeft();
      String tableName = a.getRight().getMiddle();
      scrambles.add(Pair.of(schemaName, tableName));
    }
    OlaAggregationPlan aggPlan = new OlaAggregationPlan(scrambleMeta, scrambles);
    List<Pair<ExecutableNodeBase, ExecutableNodeBase>> oldSubscriptionInformation =
        new ArrayList<>();

    // Second, according to the plan, create individual nodes that perform aggregations.
    for (int i = 0; i < aggPlan.totalBlockAggCount(); i++) {

      // copy and remove the dependency to its parents
      oldSubscriptionInformation.clear();
      AggExecutionNodeBlock copy = aggNodeBlock.deepcopyExcludingDependentAggregates(oldSubscriptionInformation);
      AggExecutionNode aggroot = (AggExecutionNode) copy.getBlockRootNode();
      for (ExecutableNodeBase parent : aggroot.getExecutableNodeBaseParents()) {
        parent.cancelSubscriptionTo(aggroot);   // not sure if this is required, but do anyway
      }
      aggroot.cancelSubscriptionsFromAllSubscribers();   // subscription will be reconstructed later.

      // Add extra predicates to restrain each aggregation to particular parts of base tables.
      List<Pair<ExecutableNodeBase, Triple<String, String, String>>> scrambledNodeAndTableName =
          identifyScrambledNodes(scrambleMeta, copy.getNodesInBlock());

      // Assign hyper table cube to the block
      aggroot.getMeta().setCubes(Arrays.asList(aggPlan.cubes.get(i)));

      // Search for agg column
      // Rewrite individual aggregate node so that it only select supported aggregate columns and non-aggregate columns
      List<SelectItem> newSelectlist = rewriteSelectlistWithBasicAgg(aggroot.getSelectQuery(),  aggroot.getMeta());

      // Add a tier column and a group attribute if the from list has multiple tier table
      addTierColumn(aggroot.getSelectQuery(), newSelectlist, scrambleMeta);

      aggroot.getSelectQuery().clearSelectList();
      aggroot.getSelectQuery().getSelectList().addAll(newSelectlist);

      aggColumnIdentiferNum = 0;

      // Insert predicates into individual aggregation nodes
      for (Pair<ExecutableNodeBase, Triple<String, String, String>> a : scrambledNodeAndTableName) {
        ExecutableNodeBase scrambledNode = a.getLeft();
        String schemaName = a.getRight().getLeft();
        String tableName = a.getRight().getMiddle();
        String aliasName = a.getRight().getRight();
        Pair<Integer, Integer> span = aggPlan.getAggBlockSpanForTable(schemaName, tableName, i);
        String aggblockColumn = scrambleMeta.getAggregationBlockColumn(schemaName, tableName);
        SelectQuery q = ((QueryNodeBase) scrambledNode).getSelectQuery();
        //        String aliasName = findAliasFor(schemaName, tableName, q.getFromList());
        if (aliasName == null) {
          throw new VerdictDBValueException(String.format("The alias name for the table (%s, %s) is not found.", schemaName, tableName));
        }

        int left = span.getLeft();
        int right = span.getRight();
        if (left == right) {
          q.addFilterByAnd(ColumnOp.equal(new BaseColumn(aliasName, aggblockColumn), ConstantColumn.valueOf(left)));
        } else {
          q.addFilterByAnd(ColumnOp.greaterequal(
              new BaseColumn(aliasName, aggblockColumn),
              ConstantColumn.valueOf(left)));
          q.addFilterByAnd(ColumnOp.lessequal(
              new BaseColumn(aliasName, aggblockColumn),
              ConstantColumn.valueOf(right)));
        }
      }

      individualAggNodes.add(aggroot);
    }


    // Third, stack combiners
    // clear existing broadcasting queues of individual agg nodes
    for (ExecutableNodeBase n : individualAggNodes) {
      n.cancelSubscriptionsFromAllSubscribers();
    }
    for (int i = 1; i < aggPlan.totalBlockAggCount(); i++) {
      AggCombinerExecutionNode combiner;
      if (i == 1) {
        combiner = AggCombinerExecutionNode.create(
            idCreator,
            individualAggNodes.get(0),
            individualAggNodes.get(1));
      } else {
        combiner = AggCombinerExecutionNode.create(
            idCreator,
            combiners.get(i-2),
            individualAggNodes.get(i));
      }
      combiners.add(combiner);
    }

    // Fourth, re-link the subscription relationship for the new AsyncAggNode
    ExecutableNodeBase newRoot = AsyncAggExecutionNode.create(idCreator, individualAggNodes, combiners, scrambleMeta);

    // Finally remove the old subscription information: old copied node -> still used old node
    for (Pair<ExecutableNodeBase, ExecutableNodeBase> parentToSource : oldSubscriptionInformation) {
      ExecutableNodeBase subscriber = parentToSource.getLeft();
      ExecutableNodeBase source = parentToSource.getRight();
      subscriber.cancelSubscriptionTo(source);
    }

    return newRoot;
  }

  /**
   * 
   * @param scrambleMeta  Information about what tables have been scrambled.
   * @param blockNodes
   * @return ExecutableNodeBase is the reference to the scrambled base table.
   *         The triple is (schema, table, alias) of scrambled tables.
   */
  static List<Pair<ExecutableNodeBase, Triple<String, String, String>>>
    identifyScrambledNodes(ScrambleMetaSet scrambleMeta, List<ExecutableNodeBase> blockNodes) {

    List<Pair<ExecutableNodeBase, Triple<String, String, String>>> identified = new ArrayList<>();

    for (ExecutableNodeBase node : blockNodes) {
      for (AbstractRelation rel : ((QueryNodeBase) node).getSelectQuery().getFromList()) {
        if (rel instanceof BaseTable) {
          BaseTable base = (BaseTable) rel;
          if (scrambleMeta.isScrambled(base.getSchemaName(), base.getTableName())) {
            identified.add(Pair.of(
                node,
                Triple.of(
                    base.getSchemaName(),
                    base.getTableName(),
                    base.getAliasName().get())));
          }
        }
        else if (rel instanceof JoinTable) {
          for (AbstractRelation r : ((JoinTable) rel).getJoinList()) {
            if (r instanceof BaseTable) {
              BaseTable base = (BaseTable) r;
              if (scrambleMeta.isScrambled(base.getSchemaName(), base.getTableName())) {
                identified.add(
                    Pair.of(
                        node,
                        Triple.of(
                            base.getSchemaName(),
                            base.getTableName(),
                            base.getAliasName().get())));
              }
            }
          }
        }
      }
    }

    return identified;
  }
  
  
  List<SelectItem> rewriteSelectlistWithBasicAgg(SelectQuery query, AggMeta meta) {
    
    List<SelectItem> selectList = query.getSelectList();
    List<String> aggColumnAlias = new ArrayList<>();
    HashMap<String, String> maxminAlias = new HashMap<>();
    List<SelectItem> newSelectlist = new ArrayList<>();
    meta.setOriginalSelectList(selectList);
    for (SelectItem selectItem : selectList) {
      if (selectItem instanceof AliasedColumn) {
        List<ColumnOp> columnOps = getAggregateColumn(((AliasedColumn) selectItem).getColumn());
        // If it contains agg columns
        if (!columnOps.isEmpty()) {
          meta.getAggColumn().put(selectItem, columnOps);
          for (ColumnOp col:columnOps) {
            if (col.getOpType().equals("avg")) {
              if (!meta.getAggColumnAggAliasPair().containsKey(
                  new ImmutablePair<>("sum", col.getOperand(0)))) {
                ColumnOp col1 = new ColumnOp("sum", col.getOperand(0));
                newSelectlist.add(new AliasedColumn(col1, "agg"+aggColumnIdentiferNum));
                meta.getAggColumnAggAliasPair().put(
                    new ImmutablePair<>("sum", col1.getOperand(0)), "agg"+aggColumnIdentiferNum);
                aggColumnAlias.add("agg"+aggColumnIdentiferNum++);
              }
              if (!meta.getAggColumnAggAliasPair().containsKey(
                  new ImmutablePair<>("count", (UnnamedColumn) new AsteriskColumn()))) {
                ColumnOp col2 = new ColumnOp("count", new AsteriskColumn());
                newSelectlist.add(new AliasedColumn(col2, "agg"+aggColumnIdentiferNum));
                meta.getAggColumnAggAliasPair().put(
                    new ImmutablePair<>("count", (UnnamedColumn) new AsteriskColumn()), "agg"+aggColumnIdentiferNum);
                aggColumnAlias.add("agg"+aggColumnIdentiferNum++);
              }
            } else if (col.getOpType().equals("count") || col.getOpType().equals("sum")){
              if (col.getOpType().equals("count") && !meta.getAggColumnAggAliasPair().containsKey(
                  new ImmutablePair<>("count", (UnnamedColumn)(new AsteriskColumn())))) {
                ColumnOp col1 = new ColumnOp(col.getOpType());
                newSelectlist.add(new AliasedColumn(col1, "agg"+aggColumnIdentiferNum));
                meta.getAggColumnAggAliasPair().put(
                    new ImmutablePair<>(col.getOpType(), (UnnamedColumn)new AsteriskColumn()), "agg"+aggColumnIdentiferNum);
                aggColumnAlias.add("agg"+aggColumnIdentiferNum++);
              }
              else if (col.getOpType().equals("sum") && !meta.getAggColumnAggAliasPair().containsKey(
                  new ImmutablePair<>(col.getOpType(), col.getOperand(0)))) {
                ColumnOp col1 = new ColumnOp(col.getOpType(), col.getOperand(0));
                newSelectlist.add(new AliasedColumn(col1, "agg"+aggColumnIdentiferNum));
                meta.getAggColumnAggAliasPair().put(
                    new ImmutablePair<>(col.getOpType(), col1.getOperand(0)), "agg"+aggColumnIdentiferNum);
                aggColumnAlias.add("agg"+aggColumnIdentiferNum++);
              }
            } else if (col.getOpType().equals("max") || col.getOpType().equals("min")) {
              ColumnOp col1 = new ColumnOp(col.getOpType(), col.getOperand(0));
              newSelectlist.add(new AliasedColumn(col1, "agg"+aggColumnIdentiferNum));
              meta.getAggColumnAggAliasPairOfMaxMin().put(
                  new ImmutablePair<>(col.getOpType(), col1.getOperand(0)), "agg"+aggColumnIdentiferNum);
              maxminAlias.put("agg"+aggColumnIdentiferNum++, col.getOpType());
            }
          }
        }
        else {
          newSelectlist.add(selectItem);
        }
      } else {
        newSelectlist.add(selectItem);
      }
    }
    meta.setAggAlias(aggColumnAlias);
    meta.setMaxminAggAlias(maxminAlias);
    return newSelectlist;
  }
  
  private List<ColumnOp> getAggregateColumn(UnnamedColumn sel) {
    List<SelectItem> itemToCheck = new ArrayList<>();
    itemToCheck.add(sel);
    List<ColumnOp> columnOps = new ArrayList<>();
    while (!itemToCheck.isEmpty()) {
      SelectItem s = itemToCheck.get(0);
      itemToCheck.remove(0);
      if (s instanceof ColumnOp) {
        if (((ColumnOp) s).getOpType().equals("count") || ((ColumnOp) s).getOpType().equals("sum") || ((ColumnOp) s).getOpType().equals("avg")
            ||((ColumnOp) s).getOpType().equals("max")||((ColumnOp) s).getOpType().equals("min")) {
          columnOps.add((ColumnOp) s);
        }
        else itemToCheck.addAll(((ColumnOp) s).getOperands());
      }
    }
    return columnOps;
  }
  

  /** identify the nodes that are 
   * (1) aggregates with scrambled tables,
   * (2) no descendants of any other top aggregates,
   * (3) the aggregated columns (inner-most base columns if they are inside some functions)
   *     must include only the columns from the scrambled tables.
   */
  private List<AggExecutionNodeBlock> identifyTopAggBlocks(ScrambleMetaSet scrambleMeta, ExecutableNodeBase root) {
    List<AggExecutionNodeBlock> aggblocks = new ArrayList<>();
    //    ScrambleMeta scrambleMeta = root.getPlan().getScrambleMeta();

    if (root instanceof AggExecutionNode) {
      // check if it contains at least one scrambled table.
      if (doesContainScramble(root, scrambleMeta)) {
        AggExecutionNodeBlock block = new AggExecutionNodeBlock(root);
        aggblocks.add(block);
        return aggblocks;
      }
    }

    for (ExecutableNodeBase dep : root.getExecutableNodeBaseDependents()) {
      List<AggExecutionNodeBlock> depAggBlocks = identifyTopAggBlocks(scrambleMeta, dep);
      aggblocks.addAll(depAggBlocks);
    }

    return aggblocks;
  }

  private boolean doesContainScramble(ExecutableNodeBase node, ScrambleMetaSet scrambleMeta) {
    SelectQuery query = ((QueryNodeBase) node).getSelectQuery();

    // check within the query
    for (AbstractRelation rel : query.getFromList()) {
      if (rel instanceof BaseTable) {
        BaseTable base = (BaseTable) rel;
        String schemaName = base.getSchemaName();
        String tableName = base.getTableName();
        if (scrambleMeta.isScrambled(schemaName, tableName)) {
          return true;
        }
      } else if (rel instanceof JoinTable) {
        for (AbstractRelation r : ((JoinTable) rel).getJoinList()) {
          if (r instanceof BaseTable) {
            BaseTable base = (BaseTable) r;
            String schemaName = base.getSchemaName();
            String tableName = base.getTableName();
            if (scrambleMeta.isScrambled(schemaName, tableName)) {
              return true;
            }
          }
        }
      }
      // SelectQuery is not supposed to be passed.
    }

    for (ExecutableNodeBase dep : node.getExecutableNodeBaseDependents()) {
      if (dep instanceof AggExecutionNode) {
        continue;
      }
      if (doesContainScramble(dep, scrambleMeta)) {
        return true;
      }
    }
    return false;
  }
  
  /**
   * Adds tier expressions to the individual aggregates
   * 
   * @param query
   * @param newSelectList
   * @param scrambleMeta
   */
  private void addTierColumn(SelectQuery query, List<SelectItem> newSelectList, ScrambleMetaSet scrambleMeta) {
    for (AbstractRelation table : query.getFromList()) {
      if (table instanceof BaseTable) {
        String schemaName = ((BaseTable) table).getSchemaName();
        String tableName = ((BaseTable) table).getTableName();
        if (scrambleMeta.isScrambled(schemaName, tableName) && 
            scrambleMeta.getMetaForTable(schemaName, tableName).getNumberOfTiers()>1) {
          newSelectList.add(new AliasedColumn(
              new BaseColumn(schemaName, tableName, table.getAliasName().get(), scrambleMeta.getTierColumn(schemaName, tableName)),
              VERDICTDB_TIER_COLUMN_NAME + verdictdbTierIndentiferNum));
          query.addGroupby(new AliasReference(VERDICTDB_TIER_COLUMN_NAME + verdictdbTierIndentiferNum++));
        }
      }
      else if (table instanceof JoinTable) {
        for (AbstractRelation jointable:((JoinTable) table).getJoinList()) {
          if (jointable instanceof BaseTable) {
            String schemaName = ((BaseTable) jointable).getSchemaName();
            String tableName = ((BaseTable) jointable).getTableName();
            if (scrambleMeta.isScrambled(schemaName, tableName) && scrambleMeta.getMetaForTable(schemaName, tableName).getNumberOfTiers()>1) {
              newSelectList.add(new AliasedColumn(
                  new BaseColumn(schemaName, tableName, jointable.getAliasName().get(), scrambleMeta.getTierColumn(schemaName, tableName)),
                  VERDICTDB_TIER_COLUMN_NAME + verdictdbTierIndentiferNum));
              query.addGroupby(new AliasReference(VERDICTDB_TIER_COLUMN_NAME + verdictdbTierIndentiferNum++));
            }
          }
        }
      }
    }
    verdictdbTierIndentiferNum = 0;
  }

}
