var _ = require('lodash'),
    dbList = require('jsDbList'),
    dataAccess = require('jsDataAccess'),
    envProvider = require('./fakeEnvironmentProvider'),
    getDescriptor = dbList.getDescriptor,
    Deferred = require("JQDeferred"),
    async = require('async');



/**
 * Provide an execution context for a given user
 * @method getContext
 * @param {string} dbCode
 * @param {string} userName
 * @param {string} role
 * @param {number} year
 * @param {number} adate
 * @return {Context}
 */
function getContext(dbCode,userName,role,year,adate){
    var def = Deferred();
    var ctx = {sqlConn: dbList.getConnection(dbCode)};
    if (ctx.sqlConn === undefined) {
        def.reject(dbCode + ' is not a valid dbCode');
        return def.promise();
    }

    Deferred.when(
        envProvider(ctx.sqlConn).getEnvironment(userName, role, year, adate),
        dbList.getDataAccess(dbCode))
        .done(function (env, DAC) {
            ctx.environment = env;

            env.field = function (key) {
                if (key === 'lu' || key === 'cu') {
                    return env.sys('login');
                }
                if (key === 'ct' || key === 'lt') {
                    return new Date();
                }
                return undefined;
            };

            ctx.dbDescriptor = getDescriptor(dbCode);
            ctx.postDataCreator = null;
            ctx.formatter = ctx.sqlConn.getFormatter();

            ctx.dbCode = dbCode;
            ctx.dataAccess = DAC;
            def.resolve(ctx);
        })
        .fail(function (err) {
            def.reject(err);
        });

    return def.promise();
}

module.exports = {
    getContext: getContext
};
