/*globals promise sqlFun Environment rollBack reject Deferred promise fail resolve done progress notify  Context*/

var dsSpace = require('jsDataSet'),
  DataSet = dsSpace.DataSet,
  DataRow = dsSpace.DataRow,
  DataTable = dsSpace.DataTable,
  dataRowState = dsSpace.dataRowState,
  OptimisticLocking = dsSpace.OptimisticLocking,
  _ = require('lodash'),
  dq = require('jsDataQuery'),
  multiSelect = require('jsMultiSelect'),
  Deferred = require("JQDeferred");

/**
 * Utility class with methods to fill a DataSet starting from a set of rows
 * @class getData
 */
function GetDataSpace() {
}

GetDataSpace.prototype = {
  constructor: GetDataSpace,
  fillDataSetByKey: fillDataSetByKey,
  fillDataSetByFilter: fillDataSetByFilter,
  getFilterByExample: getFilterByExample,
  getFilterKey: getFilterKey, //for testing purposes
  getStartingFrom: getStartingFrom, //for testing purposes
  scanTables: scanTables, //for testing purposes
  getParentRows: getParentRows, //for testing purposes
  getAllChildRows: getAllChildRows, //for testing purposes
  getRowsByFilter: getRowsByFilter, //for testing purposes
  getByKey: getByKey,//for testing purposes
  getByFilter: getByFilter //for testing purposes
};

/**
 * Assumes key = object with all key necessary fields
 * @method getFilterKey
 * @private
 * @param {Context} context
 * @param {string} tableName
 * @param {object} keyValues
 * @returns {*}
 */
function getFilterKey(context, tableName, keyValues) {
  var def = Deferred();

  context.dbDescriptor.table(tableName)
      .then(function (tableDescr) {
        var keyValue,
            kField,
            colDescriptor,
            key = tableDescr.getKey(),
            testObj = {};
        def.resolve(dq.mcmp(key, keyValues));
      })
      .fail(function (err) {
        def.reject(err);
      });
  return def.promise();
}

/**
 * Gets a a filter
 * @method getFilterByExample
 * @param {Context} context
 * @param {string} tableName
 * @param {object} example
 * @param {bool}  [useLike=false]  --if true, uses 'like' for any string comparisons
 * @return {sqlFun} DataRow obtained with the given filter
 */
function getFilterByExample(context, tableName, example, useLike){
  var def =  Deferred();
  if (useLike) {
    def.resolve(dq.mcmpLike(example));
  }
  else {
    var  fields = _.keys(example);
    if (fields.length > 0) {
      var i,
          testValues = [];
      for (i = 0; i < fields.length; i += 1) {
        testValues[i] = example[fields[i]];
      }
      def.resolve(dq.mcmp(fields,testObj));
    }
    else {
      def.resolve(dq.constant(true));
    }
  }
  return def.promise();
}
/**
 * Fill a dataset starting with a set of filtered rows in a table
 * @method getByFilter
 * @param {Context} ctx
 * @param {DataSet} ds
 * @param {DataTable} table
 * @param {sqlFun} filter
 * @return {DataRow[]} DataRow obtained with the given filter
 */
function getByFilter(ctx, ds, table, filter) {
  var def =  Deferred(), result;
  ctx.dataAccess.selectIntoTable({table: table, filter: filter, environment: ctx.environment})
    .then(function () {
      result = table.select(filter);
      if (result.length === 0) {
        def.reject('there was no row in table ' + table.name + ' filtering with ' + filter.toString());
        return;
      }
      def.resolve(result);
    })
    .fail(function (err) {
      def.reject(err);
    });
  return def.promise();
}


/**
 * Gets a single row given its key, that must be contained in key
 * @method getByKey
 * @private
 * @param {Context} ctx
 * @param {DataTable} table
 * @param {object} keyValues
 * @return {DataRow}  DataRow obtained with the given key
 */
function getByKey(ctx, table, keyValues){
  var def =  Deferred(),
    that = this;
  getFilterKey(ctx, table.name, keyValues)
    .then(function(sqlFilter){
      return that.getByFilter(ctx, table.dataset, table, sqlFilter);
    })
    .done(function(r){
      def.resolve(r[0]);
    })
    .fail(function(err){
      def.reject(err);
    });
  return def.promise();
}

/**
 * Fills a DataSet given the key of a row
 * @method fillDataSetByKey
 * @param {Context} ctx
 * @param {DataSet} ds
 * @param {DataTable} table
 * @param {object} keyValues
 * @returns {*}
 */
function fillDataSetByKey(ctx, ds, table, keyValues){
  var def =  Deferred(),
    that = this,
    result;
  that.getByKey(ctx, table, keyValues)
    .then(function(r){
      result = r;
      return that.getStartingFrom(ctx, table);
    })
    .then(function () {
      def.resolve(result);
    })
    .fail(function (err) {
      def.reject(err);
    });
  return def.promise();
}

/**
 * Fill a dataset starting with a set of filtered rows in a table
 * @method fillDataSetByFilter
 * @param {Context} ctx
 * @param {DataSet} ds
 * @param {DataTable} table
 * @param {sqlFun} filter
 * @return {DataRow[]} DataRow obtained with the given filter
 */
function fillDataSetByFilter(ctx, ds, table, filter) {
  var def =  Deferred(),
    that = this,
    result;
  that.getByFilter(ctx, ds, table, filter)
    .then(function (arr) {
      result = arr;
      return that.getStartingFrom(ctx, table);
    })
    .then(function () {
      def.resolve(result);
    })
    .fail(function (err) {
      def.reject(err);
    });
  return def.promise();
}


/**
 * Assuming that primaryTable has ALREADY been filled with data, read all childs and parents starting from
 *  rows present in primaryTable.
 * @method getStartingFrom
 * @param {Context} ctx
 * @param {DataTable} primaryTable
 * @return {*}
 */
function getStartingFrom(ctx, primaryTable){
  var visited = {},
    that = this,
    ds = primaryTable.dataset,
    toVisit = {},
    opened=false,
    def =  Deferred();
  visited[primaryTable.name] = primaryTable;
  toVisit[primaryTable.name] = primaryTable;
  ctx.dataAccess.open()
    .then(function(){
      opened= true;
      return that.scanTables(ctx, ds, toVisit, visited)
    })
    .then(function(){
      def.resolve();
    })
    .fail(function(err){
      def.reject(err);
    })
    .always(function(){
      if (opened){
        ctx.dataAccess.close();
      }
    });

  return def.promise();



}

/**
 * @method scanTables
 * @private
 * @param {Context} ctx
 * @param {DataSet} ds
 * @param {hash} toVisit
 * @param {hash} visited
 */
function scanTables(ctx, ds, toVisit, visited){
 var def =  Deferred(),
   that = this,
  nextVisit = {},//table to visit in the next step, i.e. this will be passed recursively as toVisit
  selList = []; //{Select[]}
 if (_.keys(toVisit).length === 0){
   def.resolve();
   return;
 }

  //Every child and parent tables of toVisit that aren't yet visited or toVisit become visited and nextVisit
  _.forIn(toVisit, function(table,tableName){
    _.forEach(ds.relationsByParent[tableName], function(rel){
      if (visited[rel.childTable] || toVisit[rel.childTable]){
        return;
      }
      var childTable = ds.tables[rel.childTable];
      visited[rel.childTable]= childTable;
      nextVisit[rel.childTable]= childTable;
    });
    _.forEach(ds.relationsByChild[tableName], function (rel) {
      if (visited[rel.parentTable] || toVisit[rel.parentTable]) {
        return;
      }
      var parentTable = ds.tables[rel.parentTable];
      visited[rel.parentTable] = parentTable;
      nextVisit[rel.parentTable] = parentTable;
    });
  });

  //load all rows in nextVisit
  _.forIn(toVisit, function(table,tableName ){

    if (table.rows.length===0){
      return;
    }
    //get parents of table row
    _.forEach(table.rows,function(r){
        that.getParentRows(ds, r,nextVisit,selList)
      });
    that.getAllChildRows(
        ds,
        table,
        nextVisit,
        selList);
  });

  if (selList.length === 0){
    def.resolve()
  } else {
    ctx.dataAccess.multiSelect({
      selectList: selList,
      environment: ctx.environment
    })
      .progress(function(data){ //data.tableName and data.rows are the read data
        if (data.rows) {
          ds.tables[data.tableName].mergeArray(data.rows, true);
        }
      })
      .done(function(){
        //Recursion with new parameters
        that.scanTables(ctx, ds, nextVisit, visited)
          .done(function(){
           def.resolve();
          })
          .fail(function(err){
            def.reject(err);
          })
      })
      .fail(function(err){
        def.reject(err);
      });


  }
 return def.promise();
}

/**
 * Adds select to parent rows
 * @private
 * @method getParentRows
 * @param {DataSet} ds
 * @param {DataRow} row
 * @param {object} allowed
 * @param {Select []} selList
 */
function getParentRows(ds,row, allowed, selList){
  var childTable = row.getRow().table,
    that=this;
  if (row.getRow().state === dataRowState.deleted) {
    return;
  }
  _.forEach(ds.relationsByChild[childTable.name], function(parentRel){
    var parentTable = ds.tables[parentRel.parentTable];
    if (!allowed[parentRel.parentTable]) {
      return;
    }
    var parentFilter = parentRel.getParentsFilter(row);
    if (parentFilter.isFalse){
      return;
    }
    var multiComp = new multiSelect.MultiCompare(parentRel.parentCols,
      _.map(parentRel.childCols, function(field){return row[field];})
    );

    that.getRowsByFilter(multiComp, parentTable, selList);

  })

}

/**
 * Adds select to parent rows
 * @method getAllChildRows
 * @private
 * @param {DataSet} ds
 * @param {DataTable} parentTable
 * @param {object} allowed
 * @param {Select []} selList
 */
function getAllChildRows(ds, parentTable, allowed, selList) {
  var that = this;
  _.forEach(ds.relationsByParent[parentTable.name], function (rel) {

    if (!allowed[rel.childTable]) {
      return;
    }
    var childTable = ds.tables[rel.childTable];

    _.forEach(parentTable.select(rel.activationFilter()), function (r) {
      if (r.getRow().state === dataRowState.added) return;
      var childFilter = rel.getChildsFilter(r);
      if (childFilter.isFalse) {
        return;
      }
      var multiComp = new multiSelect.MultiCompare(rel.childCols,
        _.map(rel.parentCols, function (field) {
          return r[field];
        })
      );
      that.getRowsByFilter(multiComp, childTable, selList)
    })

  });
}

/**
 * @method getRowsByFilter
 * @private
 * @param {MultiCompare} multiComp
 * @param {DataTable} table
 * @param {Select[]} selList
 */
function getRowsByFilter(multiComp, table, selList) {
  var //mergedFilter = dq.and(filter, table.staticFilter()),
    sortBy = table.orderBy();
  selList.push(new multiSelect.Select(table.columnList())
    .from(table.tableForReading())
    .intoTable(table.name)
    .staticFilter(table.staticFilter())
    .multiCompare(multiComp)
    .orderBy(table.orderBy()));
}



// exported as an object in order to do unit tests
module.exports = new GetDataSpace();
