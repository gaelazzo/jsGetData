'use strict';
var Deferred = require('JQDeferred'),
    Environment = require('./fakeEnvironment');

var allEnvironmentProvider={};

/**
 *
 * @method getEnvironmentProvider
 * @param {sqlConn} dbConnection
 * @returns {EnvironmentProvider}
 */
function getEnvironmentProvider(dbConnection){
    var connString = dbConnection.connectionString;
    if (allEnvironmentProvider[connString]){
        return allEnvironmentProvider[connString];
    }
    allEnvironmentProvider[connString] = new EnvironmentProvider(dbConnection);
    return allEnvironmentProvider[connString];
}


/**
 * @method EnvironmentProvider
 * @param {sqlConn} dbConnection
 * @constructor
 */
function EnvironmentProvider(dbConnection) {
    this.conn = dbConnection;
    this.allEnvironments = {};
}


function hashUser(login, role,year) {
    return login + '#' + role;
}



/**
 * Gets environment for a user eventually using cache
 * @param login
 * @param role
 * @param year
 * @param adate
 * @returns {*}
 */
EnvironmentProvider.prototype.getEnvironment = function (login, role, year, adate) {
    var h = hashUser(login, role),
        q = Deferred(),
        that = this;
    if (this.allEnvironments[h]) {
        q.resolve(this.allEnvironments[h]);
        return q.promise();
    }
    this.loadEnvironment(login, role, year, adate)
        .done(function(env){
            that.allEnvironments[h] = env;
            q.resolve(env);
        })
        .fail(function(err){
            q.reject(err);
        });
    return q.promise();
};

/**
 * Loads the Environment of a user, without checking cached ones
 * @method loadEnvironment
 * @param login
 * @param role
 * @param year
 * @param adate
 * @returns {*}
 */
EnvironmentProvider.prototype.loadEnvironment = function (login, role, year, adate) {
    var q =  Deferred();
    var env = new Environment();
    env.sys('esercizio', year); //todo: change into 'year'
    env.sys('login',login);
    env.sys('adate',adate);
    env.sys('role', role);

    q.resolve(env);
    return q.promise();
};

module.exports = getEnvironmentProvider;
