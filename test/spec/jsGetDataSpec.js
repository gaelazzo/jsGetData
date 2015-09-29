'use strict';
/*globals describe, beforeEach,it,expect,jasmine,spyOn */
var getData = require('../../../lib/controllers/metaData/getData'),
  getContext = require('../../../lib/controllers/metaData/context').getContext,
  MaxCacher = require('../../../lib/controllers/metaData/postData').MaxCacher,
  dsNameSpace = require('jsDataSet'),
  dq = require('jsdataquery'),
  DA = require('jsDataAccess'),
  Deferred = require("JQDeferred"),
  Environment = require('../../../lib/controllers/dataAccess/environment'),
  sqlServerDriver = require('jsSqlServerDriver'),
  dataRowState = dsNameSpace.dataRowState,
  DataSet = dsNameSpace.DataSet,
  Select = require('jsMultiSelect').Select,
  OptimisticLocking = DataSet.OptimisticLocking,
  dataSetProvider = require('../../../app/scripts/providers/dataSetProvider'),
  _ = require('lodash');

describe('getData', function () {
  var ctx,
    dsOperatore;

  beforeEach(function (done) {
    dsOperatore = dataSetProvider('operatore');
    getContext('helpdesk', 'nino', 'default', '2014', new Date(2014, 1, 20, 10, 10, 10, 0))
      .done(function (res) {
        ctx = res;
        done();
      })
      .fail(function () {
        done();
      })
  });

 it('should define getFilterKey', function(){
   expect(getData.getFilterKey).toEqual(jasmine.any(Function));
 });

  it('getFilterKey should filter key fields (single key)', function (done) {
    getData.getFilterKey(ctx, 'operatore',1)
      .done(function(filter){
        var arr = [{a: 1, idoperatore: 2},{a: 3, c: 5},{a: 4, idoperatore: 1},{a: 5, idoperatore: 6}],
          res = _.filter(arr, filter);
        expect(res).toEqual([{a: 4, idoperatore: 1}]);
        done();
      })
      .fail(function(err){
        expect(true).toBeUndefined();
        done();
      })
  });


  it('getFilterKey should filter key fields (multi key)', function (done) {
    getData.getFilterKey(ctx, 'swprocessdetail', '3§2')
      .done(function (filter) {
        var arr = [
            {a: 1, idswprocess: 1, iddetail:1},
            {a: 3, c: 5},
            {a: 4, idswprocess: 3, iddetail: 1},
            {a: 5, idswprocess: 3, iddetail: 2},
            {a: 6, idswprocess: 3, iddetail: 3}
          ],
          res = _.filter(arr, filter);
        expect(res).toEqual([
          {a: 5, idswprocess: 3, iddetail: 2}
        ]);
        done();
      })
      .fail(function (err) {
        expect(true).toBeUndefined();
        done();
      })
  });

  it('fillDataSetByKey should fill a dataset (single table)', function(done){
    getData.fillDataSetByKey(ctx, dsOperatore, dsOperatore.tables['operatore'], '48' )
      .done(function(){
        expect(dsOperatore.tables['operatore'].rows.length).toBe(1);
        expect(dsOperatore.tables['operatore'].rows[0]['idoperatore']).toBe(48);
        expect(dsOperatore.tables['operatore'].rows[0]['denominazione']).toBe('Formica Gaetano');
        done();
      })
      .fail(function(err){
        expect(err).toBeUndefined();
        done();
      })
  });

  it('fillDataSetByKey should fill main table (multiple table)', function (done) {
    var dsSwProcess = dataSetProvider('swprocess');
    getData.fillDataSetByKey(ctx, dsSwProcess, dsSwProcess.tables['swprocess'], '2700')
      .done(function () {
        expect(dsSwProcess.tables['swprocess'].rows.length).toBe(1);
        expect(dsSwProcess.tables['swprocess'].rows[0]['idswprocess']).toBe(2700);
        expect(dsSwProcess.tables['swprocess'].rows[0]['elencoregole']).toContain('GEIVA039');
        done();
      })
      .fail(function (err) {
        expect(err).toBeUndefined();
        done();
      })
  });




  it('fillDataSetByKey should call getStartingFrom)', function (done) {
    var dsSwProcess = dataSetProvider('swprocess');
    spyOn(getData,'getStartingFrom').andCallThrough();
    getData.fillDataSetByKey(ctx, dsSwProcess, dsSwProcess.tables['swprocess'], '2700')
      .done(function () {
        expect(getData.getStartingFrom).toHaveBeenCalled();
        done();
      })
      .fail(function (err) {
        expect(err).toBeUndefined();
        done();
      })
  });

  it('fillDataSetByKey should call getByKey)', function (done) {
    var dsSwProcess = dataSetProvider('swprocess');
    spyOn(getData, 'getByKey').andCallThrough();
    getData.fillDataSetByKey(ctx, dsSwProcess, dsSwProcess.tables['swprocess'], '2700')
      .done(function () {
        expect(getData.getByKey).toHaveBeenCalled();
        done();
      })
      .fail(function (err) {
        expect(err).toBeUndefined();
        done();
      })
  });

  it('fillDataSetByKey should call getStartingFrom)', function (done) {
    var dsSwProcess = dataSetProvider('swprocess');
    spyOn(getData, 'getStartingFrom').andCallThrough();
    getData.fillDataSetByKey(ctx, dsSwProcess, dsSwProcess.tables['swprocess'], '2700')
      .done(function () {
        expect(getData.getStartingFrom).toHaveBeenCalled();
        done();
      })
      .fail(function (err) {
        expect(err).toBeUndefined();
        done();
      })
  });

  it('fillDataSetByKey should call getStartingFrom with given table filled)', function (done) {
    var dsSwProcess = dataSetProvider('swprocess');
    spyOn(getData, 'getStartingFrom').andCallThrough();
    getData.fillDataSetByKey(ctx, dsSwProcess, dsSwProcess.tables['swprocess'], '2700')
      .done(function () {
        expect(getData.getStartingFrom.calls[0].args[0]).toBe(ctx);
        expect(getData.getStartingFrom.calls[0].args[1]).toBe(dsSwProcess.tables['swprocess']);
        done();
      })
      .fail(function (err) {
        expect(err).toBeUndefined();
        done();
      })
  });

  it('fillDataSetByKey should call scanTables)', function (done) {
    var dsSwProcess = dataSetProvider('swprocess');
    spyOn(getData, 'scanTables').andCallThrough();
    getData.fillDataSetByKey(ctx, dsSwProcess, dsSwProcess.tables['swprocess'], '2700')
      .done(function () {
        expect(getData.scanTables).toHaveBeenCalled();
        done();
      })
      .fail(function (err) {
        expect(err).toBeUndefined();
        done();
      })
  });


  it('fillDataSetByKey should fill child table (one child table)', function (done) {
    var dsSwProcess = dataSetProvider('swprocess');
    getData.fillDataSetByKey(ctx, dsSwProcess, dsSwProcess.tables['swprocess'], '2700')
      .done(function () {
        expect(dsSwProcess.tables['swprocessdetail'].rows.length).toBe(3);
        if (dsSwProcess.tables['swprocessdetail'].rows.length>0) {
          expect(dsSwProcess.tables['swprocessdetail'].rows[0]['idoperatore']).toBe(23);
        }
        done();
      })
      .fail(function (err) {
        expect(err).toBeUndefined();
        done();
      })
  });

  it('fillDataSetByKey should fill child table (different parent tables)', function (done) {
    var dsSwProcess = dataSetProvider('swprocess','default');
    getData.fillDataSetByKey(ctx, dsSwProcess, dsSwProcess.tables['swprocess'], '5889')
      .done(function () {
        expect(dsSwProcess.tables['swprocessdetail'].rows.length).toBe(7);
        expect(dsSwProcess.tables['operatore'].rows.length).toBe(1);
        if (dsSwProcess.tables['operatore'].rows.length>0) {
          expect(dsSwProcess.tables.operatore.rows[0].idoperatore).toBe(44);
        }
        expect(dsSwProcess.tables['telefonocliente'].rows.length).toBe(1);
        if (dsSwProcess.tables['telefonocliente'].rows.length>0) {
          expect(dsSwProcess.tables.telefonocliente.rows[0].idtipotelefono).toBe(2);
        }
        expect(dsSwProcess.tables['clienteview'].rows.length).toBe(1);
        if (dsSwProcess.tables['clienteview'].rows.length>0) {
          expect(dsSwProcess.tables.clienteview.rows[0].idcliente).toBe(1);
        }
        expect(dsSwProcess.tables['ente'].rows.length).toBe(1);
        if (dsSwProcess.tables['ente'].rows.length>0) {
          expect(dsSwProcess.tables.ente.rows[0].idente).toBe(15);
        }
        expect(dsSwProcess.tables['struttura'].rows.length).toBe(1);
        if (dsSwProcess.tables['struttura'].rows.length>0) {
          expect(dsSwProcess.tables.struttura.rows[0].idstruttura).toBe(1);
        }
        done();
      })
      .fail(function (err) {
        expect(true).toBeUndefined();
        done();
      })
  });

  it('fillDataSetByFilter should fill give same results as fillDataSetByKey when filter is key filter', function (done) {
    var dsSwProcess = dataSetProvider('swprocess', 'default'),
      dsSwProcess2 = dataSetProvider('swprocess', 'default');

    getData.fillDataSetByFilter(ctx, dsSwProcess, dsSwProcess.tables['swprocess'], dq.eq('idswprocess',5889))
      .then(function(){
        return getData.fillDataSetByKey(ctx, dsSwProcess2, dsSwProcess2.tables['swprocess'], '5889')
      })
      .done(function () {
        expect(dsSwProcess.tables['swprocessdetail'].rows).toEqual(dsSwProcess2.tables['swprocessdetail'].rows);
        expect(dsSwProcess.tables['operatore'].rows).toEqual(dsSwProcess2.tables['operatore'].rows);
        expect(dsSwProcess.tables['telefonocliente'].rows).toEqual(dsSwProcess2.tables['telefonocliente'].rows);
        expect(dsSwProcess.tables['clienteview'].rows).toEqual(dsSwProcess2.tables['clienteview'].rows);
        expect(dsSwProcess.tables['ente'].rows).toEqual(dsSwProcess2.tables['ente'].rows);
        expect(dsSwProcess.tables['struttura'].rows).toEqual(dsSwProcess2.tables['struttura'].rows);
        done();
      })
      .fail(function (err) {
        expect(true).toBeUndefined();
        expect(err).toBeUndefined();
        done();
      })
  });

  it('fillDataSetByFilter should call getParentRows on every single row read', function (done) {
    var dsSwProcess = dataSetProvider('swprocess', 'default');
    spyOn(getData, 'getParentRows').andCallThrough();
    getData.fillDataSetByFilter(ctx, dsSwProcess, dsSwProcess.tables['swprocess'], dq.eq('idswprocess', 5889))
      .done(function () {

        expect(getData.getParentRows.callCount).toEqual(
          _.reduce(dsSwProcess.tables, function (accumulator, t) {
            accumulator += t.rows.length;
            return accumulator
          }, 0)
        );
        done();
      })
      .fail(function (err) {
        expect(true).toBeUndefined();
        expect(err).toBeUndefined();
        done();
      })
  });

  it('fillDataSetByFilter should call getAllChildRows on every not-empty table read', function (done) {
    var dsSwProcess = dataSetProvider('swprocess', 'default');
    spyOn(getData, 'getAllChildRows').andCallThrough();
    getData.fillDataSetByFilter(ctx, dsSwProcess, dsSwProcess.tables['swprocess'], dq.eq('idswprocess', 5889))
      .done(function () {

        expect(getData.getAllChildRows.callCount).toEqual(
          _.reduce(dsSwProcess.tables, function (accumulator, t) {
            if (t.rows.length>0){
              accumulator += 1;
            }
            return accumulator
          }, 0)
        );
        done();
      })
      .fail(function (err) {
        expect(true).toBeUndefined();
        expect(err).toBeUndefined();
        done();
      })
  });
});
