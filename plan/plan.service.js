let util = require('util');
var cfg = require('config');
let LOG_CATEGORY = "planService";
let Q = require('q');
var client = require('../../config/elastic');
var logger = require('../../lib/logger');
const sequlizeConn = require('../../config/sequlize.connection');
const _ = require('lodash');
var esDao = require('./plan.esDao');
const validation = require('../../common_utils/validation');
const manifestSchema = require('../../models/manifest.model');
const manifestErrorLogSchema = require('../../models/manifestErrorLog.model');
const Sequelize = require('sequelize');
const Op = Sequelize.Op;
/**
 * This function is to get unplanned orders list
 * @param {number} from Page number for pagination
 * @param {number} size List size of each page for pagination
 */
var getUnplannedOrdersList = async (from, size) => {
    let logMethod = 'getUnplannedOrdersList';
    try {
        const response = await client.search({
            index: '' + cfg.indxName.unplannedOrdersINDX + '',
            from: from,
            size: size,
            type: 'doc',
            body: {
                query: {
                    "match": {
                        order_STTS: cfg.statusA
                    }
                }
            }
        });
        return {
            "orders": _.map(response.hits.hits, '_source')
        };
    } catch (err) {
        logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
        throw err;
    }
}
/**
 * Calculate weight for each manifest 
 * @param {string} manifestSourceId 
 */
var getCommodityWeight = (manifestSourceId) => {
    let logMethod = 'getPlannedOrdersCommodityWeight';
    var deferred = Q.defer();
    try {
        var promises = [
            validation.isRequired("manifestSourceId", manifestSourceId)
        ];
        Q.allSettled(promises)
            .then(function (results) {
                var msg = [];
                for (var i = 0; i < results.length; i++) {
                    if (results[i].state == 'rejected') {
                        msg.push(results[i].reason);
                    }
                }
                if (msg.length == 0) {
                    const response = client.search({
                        index: '' + cfg.indxName.unplannedOrdersINDX + '',
                        type: 'doc',
                        body: {
                            query: {
                                match: {
                                    mnfst_SRC_ID: manifestSourceId
                                }
                            },
                            aggs: {
                                result: {
                                    sum: {
                                        script: {
                                            source: "Double.parseDouble(doc['weight.keyword'].value)"
                                        }
                                    }
                                }
                            }
                        }
                    });
                    response.then(resp => {
                        deferred.resolve({
                            "status": "success",
                            "commodityWeight": resp.aggregations.result.value
                        })
                    }).catch(err => {
                        logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
                        deferred.reject(err);
                        // deferred.resolve({
                        //     "status": "error",
                        //     "message": err
                        // })
                    })
                }
            })
    } catch (err) {
        logger.info(util.format("<-%s::%s", LOG_CATEGORY, logMethod));
        logger.error(err);
        throw err;
    }
    return deferred.promise;
}

////////////////// get Pallet Count for planned Order /////////////////
var getLaneDensityPalletcount = (manifestSourceId) => {
    let logMethod = 'getLaneDensityPalletcount';
    var deferred = Q.defer();
    try {
        var promises = [
            validation.isRequired("manifestSourceId", manifestSourceId)
        ];
        Q.allSettled(promises)
            .then(function (results) {
                var msg = [];
                for (var i = 0; i < results.length; i++) {
                    if (results[i].state == 'rejected') {
                        msg.push(results[i].reason);
                    }
                }
                if (msg.length == 0) {
                    const response = client.search({
                        index: '' + cfg.indxName.unplannedOrdersINDX + '',
                        type: 'doc',
                        body: {
                            "query": {
                                "bool": {
                                    "must": [{
                                        "term": {
                                            "mnfst_SRC_ID.keyword": manifestSourceId
                                        }
                                    }]
                                }
                            },
                            "aggs": {
                                "palletCount": {
                                    "sum": {
                                        "field": "order_LN_PLET_CNT"
                                    }
                                }
                            }
                        }
                    });
                    response.then(resp => {
                        deferred.resolve({
                            "status": "success",
                            "palletCount": resp.aggregations.palletCount.value
                        })
                    }).catch(err => {
                        logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
                        deferred.reject(err);
                    })
                }
            })
    } catch (err) {
        logger.info(util.format("<-%s::%s", LOG_CATEGORY, logMethod));
        logger.error(err);
        throw err;
    }
    return deferred.promise;
}

/*
* checkFreightType for BR_PLN_7
*/
var checkFreightType = function (manifestSourceId) {
    let logMethod = 'checkFreightType';
    var deferred = Q.defer();
    try {
        var promises = [
            validation.isRequired("manifestSourceId", manifestSourceId)
        ];
        Q.allSettled(promises)
            .then(function (results) {
                var msg = [];
                for (var i = 0; i < results.length; i++) {
                    if (results[i].state == 'rejected') {
                        msg.push(results[i].reason);
                    }
                }
                if (msg.length == 0) {
                    const response = client.search({
                        index: '' + cfg.indxName.unplannedOrdersINDX + '',
                        type: 'doc',
                        body: {
                            _source: ["mnfst_SRC_ID", "frght_TYPE", "order_LN_ID"],
                            query: {
                                match: {
                                    mnfst_SRC_ID: manifestSourceId
                                }
                            }
                        }
                    })
                    response.then(result => {
                        if (result.hits.hits.length) {
                            var unplannedOrderData = result.hits.hits;
                            client.search({
                                index: '' + cfg.indxName.manifestINDX + '',
                                type: 'doc',
                                body: {
                                    _source: ["sourceId", "freightType", "id"],
                                    query: {
                                        match: {
                                            sourceId: manifestSourceId
                                        }
                                    }
                                }
                            }).then(manifestResult => {
                                if (manifestResult.hits.hits.length) {
                                    if (Object.keys(manifestResult.hits.hits[0]._source).length > 0 && !([null, undefined, ''].includes(manifestResult.hits.hits[0]._source.freightType))) {
                                        var freightType = manifestResult.hits.hits[0]._source.freightType;
                                        var status = false;
                                        unplannedOrderData.map(frghtResp => {
                                            if (frghtResp._source.frght_TYPE.toLowerCase().trim() != freightType.toLowerCase().trim()) {
                                                status = true;
                                            }
                                        })
                                        if (!status) {
                                            deferred.resolve({
                                                "status": "All good"
                                            })
                                        } else {
                                            var criticalErrorMag = "";
                                            unplannedOrderData.map(frghtResp => {
                                                criticalErrorMag += "Manifest '" + manifestSourceId + "' is an '" + freightType + "' whereas Order '" + frghtResp._source.order_LN_ID + "' is an '" + frghtResp._source.frght_TYPE + "' order" + ",";
                                            });
                                            _updateManifestErrorLog(manifestResult.hits.hits[0]._source.id, "7", criticalErrorMag).then(resp => {
                                                deferred.resolve({
                                                    "status": "Critical"
                                                })
                                            })
                                        }
                                    } else {
                                        /////////// For First time, Order move to manifest /////////////////
                                        ////////// Mysql Update for freightType ////////////////
                                        sequlizeConn.getMySQLConn().then((conn) => {
                                            return conn.transaction().then(function (t) {
                                                return manifestSchema(conn).update({
                                                    manifestFrightType: unplannedOrderData[0]._source.frght_TYPE
                                                }, {
                                                    where: {
                                                        manifestSrcId: {
                                                            [Op.eq]: manifestSourceId
                                                        }
                                                    }
                                                }, {
                                                    transaction: t
                                                }).then(function (res) {
                                                    ////////// Index Update for freightType ////////////////
                                                    return client.updateByQuery({
                                                        index: '' + cfg.indxName.manifestINDX + '',
                                                        type: 'doc',
                                                        body: {
                                                            "query": {
                                                                "match": {
                                                                    "sourceId": manifestSourceId
                                                                }
                                                            },
                                                            "script": {
                                                                "inline": "ctx._source.freightType = '" + unplannedOrderData[0]._source.frght_TYPE + "'"
                                                            }
                                                        }
                                                    });
                                                }).then(function (res) {
                                                    var status = false;
                                                    unplannedOrderData.map(frghtResp => {
                                                        if (frghtResp._source.frght_TYPE.toLowerCase().trim() != unplannedOrderData[0]._source.frght_TYPE.toLowerCase().trim()) {
                                                            status = true;
                                                        }
                                                    })
                                                    if (!status) {
                                                        deferred.resolve({
                                                            "status": "matched"
                                                        })
                                                    } else {
                                                        var criticalErrorMag = "";
                                                        unplannedOrderData.map(frghtResp => {
                                                            criticalErrorMag += "Manifest '" + manifestSourceId + "' is an '" + freightType + "' whereas Order '" + frghtResp._source.order_LN_ID + "' is an '" + frghtResp._source.frght_TYPE + "' order" + ",";
                                                        });
                                                        _updateManifestErrorLog(manifestResult.hits.hits[0]._source.id, "7", criticalErrorMag).then(resp => {
                                                            deferred.resolve({
                                                                "status": "Critical"
                                                            })
                                                        })
                                                    }
                                                    t.commit();
                                                }).catch((err) => {
                                                    t.rollback();
                                                    deferred.reject(err);
                                                });
                                            });
                                        });
                                    }
                                }
                            }).catch(err => {
                                logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
                                deferred.reject(err);
                            });
                        }
                    }).catch(err => {
                        logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
                        deferred.reject("There is no manifest");
                    });
                } else {
                    logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
                    deferred.reject(err);
                }
            })
    } catch (err) {
        logger.info(util.format("<-%s::%s", LOG_CATEGORY, logMethod));
        logger.error(err);
        throw err;
    }
    return deferred.promise;
}

/**
 * check Load type for BR_PLN_6
 */
var checkLoadTypeManifest = function (manifestSourceId) {
    var logMethod = 'checkLoadTypeManifest';
    var deferred = Q.defer();
    try {
        var promises = [
            validation.isRequired("manifestSourceId", manifestSourceId)
        ];
        Q.allSettled(promises)
            .then(function (results) {
                var msg = [];
                for (var i = 0; i < results.length; i++) {
                    if (results[i].state == 'rejected') {
                        msg.push(results[i].reason);
                    }
                }
                if (msg.length == 0) {
                    const response = client.search({
                        index: '' + cfg.indxName.unplannedOrdersINDX + '',
                        type: 'doc',
                        body: {
                            _source: ["mnfst_SRC_ID", "order_LN_LOAD_TYPE", "order_LN_ID"],
                            query: {
                                match: {
                                    mnfst_SRC_ID: manifestSourceId
                                }
                            }
                        }
                    })
                    response.then(result => {
                        if (result.hits.hits.length) {
                            var loadData = result.hits.hits;
                            client.search({
                                index: '' + cfg.indxName.manifestINDX + '',
                                type: 'doc',
                                body: {
                                    _source: ["sourceId", "loadType", "id"],
                                    query: {
                                        match: {
                                            sourceId: manifestSourceId
                                        }
                                    }
                                }
                            }).then(manifestResult => {
                                if (manifestResult.hits.hits.length) {
                                    if (Object.keys(manifestResult.hits.hits[0]._source).length >= 0 && !([undefined].includes(manifestResult.hits.hits[0]._source.loadType))) {
                                        var loadType = manifestResult.hits.hits[0]._source.loadType;
                                        var status = false;
                                        loadData.map(loadResp => {
                                            if (loadResp._source.order_LN_LOAD_TYPE != loadType) {
                                                status = true;
                                            }
                                        })
                                        if (!status) {
                                            deferred.resolve({
                                                "status": "All good"
                                            })
                                        } else {
                                            var warningErrorMag = "";
                                            loadData.map(loadResp => {
                                                warningErrorMag += "Manifest '" + manifestSourceId + "' is an '" + loadType + "' whereas Order '" + loadResp._source.order_LN_ID + "' is an '" + loadResp._source.order_LN_LOAD_TYPE + "' order" + ",";
                                            });
                                            _updateManifestErrorLog(manifestResult.hits.hits[0]._source.id, "6", warningErrorMag).then(resp => {
                                                deferred.resolve({
                                                    "status": "Warning"
                                                })
                                            })
                                        }
                                    }
                                    /*
                                    else {
                                        /////////// For First time, Order move to manifest /////////////////
                                        ////////// Mysql Update for load Type ////////////////
                                        sequlizeConn.getMySQLConn().then((conn) => {
                                            return conn.transaction().then(function (t) {
                                                return manifestSchema(conn).update({
                                                    manifestLoadType: loadData[0]._source.order_LN_LOAD_TYPE
                                                }, {
                                                    where: {
                                                        manifestSrcId: {
                                                            [Op.eq]: manifestSourceId
                                                        }
                                                    }
                                                }, {
                                                    transaction: t
                                                }).then(function (res) {
                                                    ////////// Index Update for load Type ////////////////
                                                    return client.updateByQuery({
                                                        index: '' + cfg.indxName.manifestINDX + '',
                                                        type: 'doc',
                                                        body: {
                                                            "query": {
                                                                "match": {
                                                                    "sourceId": manifestSourceId
                                                                }
                                                            },
                                                            "script": {
                                                                "inline": "ctx._source.freightType = '" + loadData[0]._source.order_LN_LOAD_TYPE + "'"
                                                            }
                                                        }
                                                    });
                                                }).then(function (res) {
                                                    var status = false;
                                                    loadData.map(loadResp => {
                                                        if (loadResp._source.order_LN_LOAD_TYPE != loadData[0]._source.order_LN_LOAD_TYPE) {
                                                            status = true;
                                                        }
                                                    })
                                                    if (!status) {
                                                        deferred.resolve({
                                                            "status": "All good"
                                                        })
                                                    } else {
                                                        var warningErrorMag = "";
                                            loadData.map(loadResp => {
                                                warningErrorMag += "Manifest '"+ manifestSourceId + "' is an '"+loadType + "' whereas Order '" + loadResp._source.order_LN_ID + "' is an '" + loadResp._source.order_LN_LOAD_TYPE + "' order" + ",";
                                            });
                                            _updateManifestErrorLog(manifestResult.hits.hits[0]._source.id, "6", warningErrorMag).then(resp => {
                                                deferred.resolve({
                                                    "status": "Warning"
                                                })
                                            })
                                                    }
                                                    t.commit();
                                                }).catch((err) => {
                                                    t.rollback();
                                                    deferred.reject(err);
                                                });
                                            });
                                        });
                                    }
                                    */
                                }
                            }).catch(err => {
                                logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
                                deferred.reject(err);
                            });
                        }
                    }).catch(err => {
                        logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
                        deferred.reject("There is no manifest");
                    });

                }

            });

    } catch (err) {
        logger.info(util.format("<-%s::%s", LOG_CATEGORY, logMethod));
        logger.error(err);
        throw err;
    }
    return deferred.promise;
}
/**
 * This function is to remove orders from manifest
 */
let removeOrderFromManifest = function (orderlineId, manifestSrcId) {
    var logMethod = "removeOrderFromManifest";
    logger.info(util.format("%s::%s->", LOG_CATEGORY, logMethod));
    var deferred = Q.defer();
    try {
        var promises = [
            validation.isRequired("orderlineId", orderlineId),
            validation.isRequired("manifestSourceId", manifestSrcId)
        ];
        Q.allSettled(promises)
            .then(function (results) {
                var msg = [];
                for (var i = 0; i < results.length; i++) {
                    if (results[i].state == 'rejected') {
                        msg.push(results[i].reason);
                    }
                }
                if (msg.length == 0) {
                    sequlizeConn.getMySQLConn().then((conn) => {
                        return conn.transaction().then(function (t) {
                            return orderlineSchema(conn).update({
                                manifestSourceId: manifestSrcId,
                                orderlineStatus: cfg.statusP
                            },{
                                where: {
                                    orderlineId: {
                                        [Op.in]: orderlineId
                                    }
                                }
                            },{
                                transaction: t
                            }).then(function (res) {
                                    return client.updateByQuery({
                                        index: '' + cfg.indxName.unplannedOrdersINDX + '',
                                        type: 'doc',
                                        body: {
                                            "query": {
                                                "bool": {
                                                    "filter": {
                                                        "terms": {
                                                            "order_LN_ID": orderlineId
                                                        }
                                                    }
                                                }
                                            },
                                            "script": {
                                                "inline": "ctx._source.order_STTS = '" + cfg.statusP + "'; ctx._source.mnfst_SRC_ID = '" + manifestSrcId + "'"
                                            }
                                        }
                                    });
                                }).then(function (res) {
                                    t.commit();
                                    return _checkBusinessRule(manifestSrcId)
                                }).then((res) => {
                                    deferred.resolve({
                                        status: "success",
                                        message: "Remove Order Successfully"
                                    });
                                }).catch((err) => {
                                    t.rollback();
                                    deferred.reject(err);
                                });
                        });
                    });
                } else {
                    logger.info(util.format("<-%s::%s", LOG_CATEGORY, logMethod));
                    logger.error(msg);
                    deferred.reject(msg);
                }
            });

    } catch (err) {
        logger.info(util.format("<-%s::%s", LOG_CATEGORY, logMethod));
        logger.error(err);
        throw err;
    }
    return deferred.promise;
}

let updateOrderDetails = function (orderId, body) {
    let logMethod = "updateOrderDetails";
    logger.info(util.format("%s::%s->", LOG_CATEGORY, logMethod));
    var deferred = Q.defer();
    let errMsg = "";

    let params = {
        orderId: orderId,
        orderLineId: body.order_LN_ID,
        pickupStartTime: body.order_LN_PCKUP_STRT_TM,
        pickupEndTime: body.order_LN_PCKUP_END_TM,
        deliveryStartTime: body.order_LN_DLVRY_STRT_TM,
        deliveryEndTime: body.order_LN_DLVRY_END_TM,
        weight: body.weight,
        palletCount: body.order_LN_PLET_CNT,
        commodityId: body.cmdty_ID,
        dispatchComment: body.order_LN_DLVRY_CMNT,
        dispatchInstructions: body.order_LN_DLVRY_INSTR,
        maxTemp: body.order_LN_MAX_TEMP,
        minTemp: body.order_LN_MIN_TEMP,
        priority: body.order_PRTY_TYPE,
        revenueMiles: body.rvnu_MI,
        revenueQuantity: body.rvnu_QTY,
        updateDate: new Date().toLocaleString(),
        updateUserId: 9999
    }
    //Need to remove UserId from above json.
    esDao.esGetUnplannedOrder(params.orderLineId).then(function (orderResult) {
        let unplannedorderdetails = orderResult[0];
        sequlizeConn.getMySQLConn().then((dbConnection) => {
            dbConnection.transaction(function (t) {
                let transObj = {
                    transaction: t
                };
                return esDao.esUpdateOrderDetails(params, transObj).then(function (result) {
                    return _dbUpdateOrderDetails(params, dbConnection, transObj).then(function (res) {
                        logger.info(util.format("<-%s::%s", LOG_CATEGORY, logMethod));
                        t.commit();
                        deferred.resolve("Order details updated successfully.");

                    }).catch(function (err) {
                        logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
                        t.rollback();
                        deferred.reject(err);
                    });
                }).catch(function (err) {
                    logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));

                    //Revert back es update ;
                    let existingParams = {
                        orderId: unplannedorderdetails.order_ID,
                        orderLineId: unplannedorderdetails.order_LN_ID,
                        pickupStartTime: unplannedorderdetails.order_LN_PCKUP_STRT_TM,
                        pickupEndTime: unplannedorderdetails.order_LN_PCKUP_END_TM,
                        deliveryStartTime: unplannedorderdetails.order_LN_DLVRY_STRT_TM,
                        deliveryEndTime: unplannedorderdetails.order_LN_DLVRY_END_TM,
                        weight: unplannedorderdetails.weight,
                        palletCount: unplannedorderdetails.order_LN_PLET_CNT,
                        commodityId: unplannedorderdetails.cmdty_ID,
                        dispatchComment: unplannedorderdetails.order_LN_DLVRY_CMNT,
                        dispatchInstructions: unplannedorderdetails.order_LN_DLVRY_INSTR,
                        maxTemp: unplannedorderdetails.order_LN_MAX_TEMP,
                        minTemp: unplannedorderdetails.order_LN_MIN_TEMP,
                        priority: unplannedorderdetails.order_PRTY_TYPE,
                        revenueMiles: unplannedorderdetails.rvnu_MI,
                        revenueQuantity: unplannedorderdetails.rvnu_QTY,
                        updateDate: unplannedorderdetails.updateDate,
                        updateUserId: unplannedorderdetails.updateUserId
                    }
                    t.rollback();
                    esDao.esUpdateOrderDetails(existingParams).then(function (result) {
                        logger.info(util.format("%s::%s: Info:%s", LOG_CATEGORY, logMethod, util.inspect(" Es Rollback completed successfully")));
                        errMsg = "Update of order details failed..";
                        deferred.reject(errMsg);
                    }).catch(function (err) {
                        logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
                        deferred.reject(errMsg);
                    });
                });
            }).then((res) => {
                deferred.resolve("Order details updated successfully.");
            }).catch(function (err) {
                logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
                deferred.reject(errMsg);
            });
        }).catch(function (err) {
            logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
            deferred.reject(errMsg);
        });
    }).catch((err) => {
        logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
        deferred.reject(errMsg);
    });

    return deferred.promise;
}

let _dbUpdateOrderDetails = function (params, dbConnection, transObj) {
    let logMethod = "_dbUpdateOrderDetails";
    logger.info(util.format("%s::%s->", LOG_CATEGORY, logMethod));
    var deferred = Q.defer();
    try {
        var inputParm = JSON.stringify(params)
        dbConnection.query('Select  fn_Update_Order_Details (?);', {
            replacements: [inputParm],
            type: dbConnection.QueryTypes.SELECT
        }, transObj).then(function (res) {
            if (res && res.length > 0) {
                logger.info("order details updated in mySql DB  successfully.");
                let p = Object.values(res[0]);
                deferred.resolve(JSON.parse(p));
            } else {
                logger.info("No records updated .");
                deferred.resolve("No records updated .")
            }

        }).catch(err => {
            logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
            deferred.reject(err);
        });
    } catch (err) {
        logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
        throw new error(err);
    }

    return deferred.promise;
}

let updateOrderSequence = function (body) {
    let logMethod = "updateOrderSequence";
    logger.info(util.format("%s::%s->", LOG_CATEGORY, logMethod));
    var deferred = Q.defer();
    try {
        sequlizeConn.getMySQLConn().then((dbConnection) => {
            dbConnection.transaction(function (t) {
                let transObj = {
                    transaction: t
                };
                return esDao.esUpdateOrderSeq(body, transObj).then((result) => {
                    return _dbUpdateOrderSeq(body, dbConnection, transObj).then((res) => {
                        t.commit();
                        deferred.resolve("order sequence updated successfully..");
                    }).catch((err) => {
                        logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
                        t.rollback();
                        deferred.reject(err);
                    });
                }).catch((err) => {
                    logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
                    t.rollback();
                    deferred.reject(err);
                });


            }).then((res) => {
                logger.info(util.format("<-%s::%s", LOG_CATEGORY, logMethod));
                deferred.resolve("Order Sequance details updated successfully.");
            }).catch(function (err) {
                logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
                deferred.reject(err);
            });
        }).catch((err) => {
            logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
            deferred.reject(err);
        });

    } catch (error) {
        logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
        throw new error(err);
    }
    return deferred.promise;
}

let _dbUpdateOrderSeq = function (body, dbConnection, transObj) {
    let logMethod = "_dbUpdateOrderSeq";
    logger.info(util.format("%s::%s->", LOG_CATEGORY, logMethod));
    var deferred = Q.defer();
    try {
        var inputParm = JSON.stringify(body);
        dbConnection
            .query('Select  fn_Update_Order_Seq (?);', {
                replacements: [inputParm],
                type: dbConnection.QueryTypes.SELECT
            }, transObj).then(function (res) {
                if (res && res.length > 0) {
                    logger.info("order Sequence details updated  successfully.");
                    let p = Object.values(res[0]);
                    deferred.resolve(p);
                }
            }).catch(err => {
                logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
                deferred.reject(err);
            });

    } catch (error) {
        logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
        throw new error(err);
    }
    return deferred.promise;
}


let getOrderById = async (id) => {
    let logMethod = "getOrderById";
    logger.info(util.format("%s::%s->", LOG_CATEGORY, logMethod));
    try {
        const response = await client.search({
            index: '' + cfg.indxName.unplannedOrdersINDX + '',
            type: 'doc',
            body: {
                query: {
                    match: {
                        order_ID: id
                    }
                }
            }
        });
        return {
            "order": _.map(response.hits.hits, '_source')
        };
    } catch (err) {
        logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
        throw err;
    }
}

let _updateManifestErrorLog = function (manifestId, manifestRuleId, manifestRuleMsg) {
    let logMethod = "_updateManifestErrorLog";
    logger.info(util.format("%s::%s->", LOG_CATEGORY, logMethod));
    var deferred = Q.defer();
    try {
        sequlizeConn.getMySQLConn().then((conn) => {
            return conn.transaction().then(function (t) {
                return manifestErrorLogSchema(conn).destroy({
                    where: {
                        [Op.and]: [{
                            manifestSrcId: manifestId
                        }, {
                            manifestRuleId: manifestRuleId
                        }]
                    }
                }, {
                    transaction: t
                }).then(function (res) {
                    return manifestErrorLogSchema(conn).create({
                        manifestSrcId: manifestId,
                        manifestRuleId: manifestRuleId,
                        manifestRuleMsg: manifestRuleMsg
                    }, {
                        transaction: t
                    }).then(function (res) {
                        t.commit();
                        deferred.resolve({
                            "status": "Error log Updated"
                        })
                    }).catch(err => {
                        t.rollback();
                        deferred.reject(err);
                    })
                })
            })
        })

    } catch (error) {
        logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
        throw new error(err);
    }
    return deferred.promise;
}

let test = (req, res, next) => {
    let logMethod = 'getUnplannedOrdersList';
    var deferred = Q.defer();
    try {
        const response = client.updateByQuery({
            index: '' + cfg.indxName.manifestINDX + '',
            type: 'doc',
            body: {
                "query": {
                    "match": {
                        "sourceId": "M108265"
                    }
                },
                "script": {
                    "inline": "ctx._source.freightType = null"
                }
            }
        })
        response.then(resp => {
            deferred.resolve(resp);
        }).catch(err => {
            deferred.resolve(err);
        })
    } catch (err) {
        logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
        throw err;
    }
    return deferred.promise;
}
// Exporting modules
module.exports = {
    getUnplannedOrdersList: getUnplannedOrdersList,
    updateOrderDetails: updateOrderDetails,
    updateOrderSequence: updateOrderSequence,
    getOrderById: getOrderById,
    getCommodityWeight: getCommodityWeight,
    getLaneDensityPalletcount: getLaneDensityPalletcount,
    checkFreightType: checkFreightType,
    checkLoadTypeManifest: checkLoadTypeManifest,
    removeOrderFromManifest: removeOrderFromManifest,
    test: test
}