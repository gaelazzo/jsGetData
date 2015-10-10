var dsSpace = require('jsDataSet');
var _ = require('lodash');


var
    DataSet = dsSpace.DataSet,
    DataRow = dsSpace.DataRow,
    DataTable = dsSpace.DataTable,
    dataRowState = dsSpace.dataRowState,
    OptimisticLocking = dsSpace.OptimisticLocking,
    optimistic = new OptimisticLocking(['lt', 'lu'], ['ct', 'cu', 'lt', 'lu']);


function getDataSetCustomer() {
    var ds = new DataSet('customer'),
        operatore = ds.newTable('customer');
    ds.optimisticLocking = optimistic;
    operatore.key(['idcustomer']);
    operatore.autoIncrement('idcustomer', {});
    return ds;
}

function getDataSetCustomerWithPhone() {
    var ds = new DataSet('customer2'),
        customer = ds.newTable('customer'),
        customerphone = ds.newTable('customerphone');
    ds.optimisticLocking = optimistic;
    customer.key(['idcustomer'])
        .autoIncrement('idcustomer', {});
    customerphone
        .key(['idcustomer', 'idcustomerphone'])
        .autoIncrement('idcustomerphone', {selector: ['idcustomer']});
    ds.newRelation('one', 'customer', ['idcustomer'], 'customerphone');
    return ds;
}

function getDataSetSell(editType) {
    var ds = new DataSet('sell'),
        customer = ds.newTable('customer'),
        customerphone = ds.newTable('customerphone');
    ds.optimisticLocking = optimistic;
    customer.key(['idcustomer'])
        .autoIncrement('idcustomer', {});
    customerphone
        .key(['idcustomer', 'idcustomerphone'])
        .autoIncrement('idcustomerphone', {selector: ['idcustomer']});
    ds.newRelation('one', 'customer', ['idcustomer'], 'customerphone');

    if (editType === 'default') {
        var op = ds.newTable('sell').key(['idsell']),
            op1 = ds.newTable('seller1').key(['idseller']).tableForReading('seller'),
            op2 = ds.newTable('seller2').key(['idseller']).tableForReading('seller'),
            op3 = ds.newTable('seller3').key(['idseller']).tableForReading('seller'),
            op4 = ds.newTable('seller4').key(['idseller']).tableForReading('seller'),
            act = ds.newTable('selleractivity').key(['idseller','idactivity']),
            sup = ds.newTable('sellsupplement').key(['idsell','idsupplement']),
            kind1 = ds.newTable('sellerkind1').key(['idsellerkind']).tableForReading('sellerkind'),
            kind2 = ds.newTable('sellerkind2').key(['idsellerkind']).tableForReading('sellerkind'),
            kind3 = ds.newTable('sellerkind3').key(['idsellerkind']).tableForReading('sellerkind'),
            kind4 = ds.newTable('sellerkind4').key(['idsellerkind']).tableForReading('sellerkind'),
            sellview = ds.newTable('sellview').key(['idsell']);

        ds.newRelation('oprel', 'seller1', ['idseller'], 'sell');
        ds.newRelation('oprel1', 'seller2', ['idseller'], 'sell',['idcoseller']);
        ds.newRelation('oprel2', 'seller3', ['idseller'], 'sell',['idcoseller2']);
        ds.newRelation('oprel3', 'seller4', ['idseller'], 'sellsupplement', ['idselleraux']);
        ds.newRelation('supp', 'sell', ['idsell'], 'sellsupplement');
        ds.newRelation('sk1', 'sellerkind1', ['idsellerkind'], 'seller1');
        ds.newRelation('sk2', 'sellerkind2', ['idsellerkind'], 'seller2');
        ds.newRelation('sk3', 'sellerkind3', ['idsellerkind'], 'seller3');
        ds.newRelation('sk4', 'sellerkind4', ['idsellerkind'], 'seller4');
        ds.newRelation('act', 'seller1', ['idseller'], 'selleractivity');
        ds.newRelation('sell_cust', 'customer', ['idcustomer'], 'sell');



        ds.newRelation('sview', 'sellview', ['idsell'], 'sell');

    }
    return ds;
}

function getDataSet(table, editType) {
    if (table === 'customer') {
        return getDataSetCustomer();
    }
    if (table === 'customerphone') {
        return getDataSetCustomerWithPhone();
    }

    if (table === 'sell') {
        return getDataSetSell(editType);
    }
}

module.exports = getDataSet;