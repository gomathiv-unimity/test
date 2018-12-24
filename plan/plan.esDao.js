var client = require('../../config/elastic');
var logger = require('../../lib/logger');
let LOG_CATEGORY = "plan.esDao";
let util = require("util");
let Q = require('q');
var cfg = require('config');
const _ = require('lodash');
let deferredNew = require('deferred');

let esUpdateOrderDetails = function (params) {
    var logMethod = "esUpdateOrderDetails";
    var deferred = Q.defer();
    //TODO:Need to chnge Update userID value below
    let orderLineUpdate_Script = {
        "inline":
            "ctx._source.pickupStartTime = '" + params.pickupStartTime + "';" +
            " ctx._source.pickupEndTime = '" + params.pickupEndTime + "';" +
            "ctx._source.deliveryStartTime = '" + params.deliveryStartTime + "';" +
            " ctx._source.deliveryEndTime = '" + params.deliveryEndTime + "';" +
            "ctx._source.weight = '" + params.weight + "';" +
            " ctx._source.palletCount = '" + params.palletCount + "';" +
            " ctx._source.commodityId = '" + params.commodityId + "';" +
            " ctx._source.deliveryComment = '" + params.dispatchComment + "';" +
            "ctx._source.deliveryInstruction = '" + params.dispatchInstructions + "';" +
            " ctx._source.minTemp = '" + params.minTemp + "';" +
            " ctx._source.maxTemp = '" + params.maxTemp + "';" +
            // " ctx._source.updateDate = '" + new Date().toLocaleString() + "';" +
            " ctx._source.updateUserId = '" + params.updateUserId + "'"
    };


    let orderUpdate_Script = {
        "inline":
            " ctx._source.revenueMiles = '" + params.revenueMiles + "';" +
            " ctx._source.revenueQuantity = '" + params.revenueQuantity + "';" +
            " ctx._source.priority = '" + params.priority + "';" +
            //" ctx._source.updateDate = '" + new Date().toLocaleString() + "';" +
            " ctx._source.updateUserId = '" + params.updateUserId + "'"
    }


    client.updateByQuery({
        index: cfg.indxName.orderLineINDX,
        type: 'doc',
        body: {
            "query": { "term": { "id": params.orderLineId } },
            "script": orderLineUpdate_Script
        }
    }).then((res) => {
        //ORDER line Table updated..
        logger.info(util.format("OrderLine Updated SuccessFully"));
        client.updateByQuery({
            index: cfg.indxName.orderINDX,
            type: 'doc',
            body: {
                "query": { "term": { "id": params.orderId } },
                "script": orderUpdate_Script
            }
        }).then((result) => {
            logger.info(util.format("<-%s::%s: Info:%s", LOG_CATEGORY, logMethod, "Order table Updated Successfully"));
            deferred.resolve(result);
        }).catch((err) => {
            logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
            deferred.reject(err);
        });
    }).catch((err) => {
        logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
        deferred.reject(err);
    });
    return deferred.promise;
}

let esGetUnplannedOrder = function (orderLineId) {
    let logMethod = "esGetUnplannedOrder";
    logger.info(util.format("%s::%s->", LOG_CATEGORY, logMethod));
    var deferred = Q.defer();
    try {
        client.search({
            index: '' + cfg.indxName.unplannedOrdersINDX + '',
            type: 'doc',
            body: {
                query: {
                    term: {
                        order_LN_ID: orderLineId
                    }
                }
            }
        }).then((result) => {
            if (result) {
                let ordetails = _.map(result.hits.hits, '_source');
                deferred.resolve(ordetails);
            }
        }).catch((err) => {
            logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
            deferred.reject(err);
        });

    } catch (err) {
        logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
        throw err;
    }
    return deferred.promise;
}


let esUpdateOrderSeq = function (bodyParams, transObj) {
    let logMethod = "esGetUnplannedOrder";
    logger.info(util.format("%s::%s->", LOG_CATEGORY, logMethod));
    var deferred = Q.defer();
    try {

        deferredNew.map(bodyParams, deferredNew.gate(function (params) {
            return _esUpdateOrderSeqById(params.orderId, params.sequence, transObj)
        }, 1))(function (result) {
            deferred.resolve(result)
        });

    } catch (err) {
        logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
        throw err;
    }
    return deferred.promise;
}
let _esUpdateOrderSeqById = function (orderId, sequence) {
    let logMethod = "_esUpdateSeqById";
    logger.info(util.format("%s::%s->", LOG_CATEGORY, logMethod));
    var deferred = Q.defer();
    //TODO:updateUserID needs to be dynamic .ll change after user module  Done. 
    let orderUpdate_Script = {
        "inline":
            " ctx._source.sequence = '" + sequence + "';" +
            //" ctx._source.updateDate = '" + new Date().toLocaleString() + "';" +
            " ctx._source.updateUserId = '" + 9999 + "'"
    }

    client.updateByQuery({
        index: cfg.indxName.orderINDX,
        type: 'doc',
        body: {
            "query": { "term": { "id": orderId } },
            "script": orderUpdate_Script
        }
    }).then((result) => {
        deferred.resolve("Seq Updated for orderId :%s", orderId);
    })
    return deferred.promise;
}
// Exporting modules
module.exports = {
    esUpdateOrderDetails: esUpdateOrderDetails,
    esGetUnplannedOrder: esGetUnplannedOrder,
    esUpdateOrderSeq: esUpdateOrderSeq
}