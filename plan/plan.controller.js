// Import required modules
var planService = require('./plan.service');
var logger = require('../../lib/logger');
let util = require('util');
let _ = require("lodash");
const commonUtils = require('../../common_utils/common');
let LOG_CATEGORY = "planControler";
/**
 * This function is to get unplanned orders list
 * @param {Object} req Request object
 * @param {Object} res Response object
 * @param {function} next callback function
 */
var unplannedOrdersList = async (req, res, next) => {
    let logMethod = "getUnplannedOrdersList";
    try {
        if (req.query && _.isEmpty(req.query)) {
            throw new Error("Request query should not be empty.");
        }
        if (!req.query.from && !req.query.size) {
            throw new Error('Pagination parameters required.');
        }
        let from = req.query.from;
        let size = req.query.size;
        const result = await planService.getUnplannedOrdersList(from, size);
        logger.info(util.format("<-%s::%s", LOG_CATEGORY, logMethod));
        commonUtils.okResponseHandler(result, req, res, next);
    } catch (err) {
        logger.error(util.format("<-%s::%s", LOG_CATEGORY, logMethod, util.inspect(err)));
        // next(err);
        res.boom.badRequest(err.message);
    }
}
var CommodityWeight = async (req, res, next) => {
    let logMethod = "getPlannedOrdersCommodityWeight";
    try {
        const result = await planService.getCommodityWeight(req.body.manifestSourceId);
        logger.info(util.format("<-%s::%s", LOG_CATEGORY, logMethod));
        commonUtils.okResponseHandler(result, req, res, next);
    } catch (err) {
        logger.info(util.format("<-%s::%s", LOG_CATEGORY, logMethod));
        logger.error(err);
        res.boom.badRequest('Internal error');
    }
}
var laneDensityPalletcount = async (req, res, next) => {
    let logMethod = "getlanedensitypalletcount";
    try {
        const result = await planService.getLaneDensityPalletcount(req.body.manifestSourceId);
        logger.info(util.format("<-%s::%s", LOG_CATEGORY, logMethod));
        commonUtils.okResponseHandler(result, req, res, next);
    } catch (err) {
        logger.info(util.format("<-%s::%s", LOG_CATEGORY, logMethod));
        logger.error(err);
        res.boom.badRequest('Internal error');
    }
}
let checkFreightType = async (req, res, next) => {
    let logMethod = 'checkFreightType';
    try {
        var manifestSourceId = (req.body.manifestSourceId) ? req.body.manifestSourceId : '';
        const result = await planService.checkFreightType(manifestSourceId);
        logger.info(util.format("<-%s::%s", LOG_CATEGORY, logMethod));
        commonUtils.okResponseHandler(result, req, res, next);
    } catch (err) {
        logger.info(util.format("<-%s::%s", LOG_CATEGORY, logMethod));
        logger.error(err);
        res.boom.badRequest(err);
    }
}
var checkLoadTypeManifest = async (req, res, next) => {
    let logMethod = "checkloadtypemanifest";
    try {
        const result = await planService.checkLoadTypeManifest(req.body.manifestSourceId);
        logger.info(util.format("<-%s::%s", LOG_CATEGORY, logMethod));
        commonUtils.okResponseHandler(result, req, res, next);
    } catch (err) {
        logger.info(util.format("<-%s::%s", LOG_CATEGORY, logMethod));
        logger.error(err);
        res.boom.badRequest('Internal error');
    }
}
/**
 * This function is to remove orders from manifest
 * @param {Object} req Request object
 * @param {Object} res Response object
 * @param {function} next callback function
 */

let removeOrderFromManifest = function (req, res, next) {
    let logMethod = "updateOrderDetails";
    logger.info(util.format("%s::%s->", LOG_CATEGORY, logMethod));
    try {
        if (req.body && _.isEmpty(req.body)) {
            throw new Error("Request body should not be empty.");
        }
        if (req.body.orderlineId == "") {
            throw new Error("Orderline id is required.");
        }
        if (req.body.manifestSrcId == "") {
            throw new Error("Manifest source id is required.");
        }
        planService.removeOrderFromManifest(req.body.orderlineId, req.body.manifestSrcId).then(function (result) {
            logger.info(util.format("<-%s::%s", LOG_CATEGORY, logMethod));
            commonUtils.okResponseHandler(result, req, res, next);
        }).catch(function (err) {
            logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, JSON.stringify(err)));
            next(err);
        });

    }
    catch (err) {
        logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, JSON.stringify(err)));
        next(err);
    }
}
let updateOrderDetails = function (req, res, next) {
    let logMethod = "updateOrderDetails";
    logger.info(util.format("%s::%s->", LOG_CATEGORY, logMethod));
    try {
        if (!req.body) {
            throw new Error("Body parameters required.");
        }
        if (req.body && _.isEmpty(req.body)) {
            throw new Error("Request body should not be  empty ");
        }
        if (req.params.orderId == "") {
            throw new Error("orderId is required. ");
        }
        planService.updateOrderDetails(req.params.orderId, req.body)
            .then(function (result) {
                logger.info(util.format("<-%s::%s", LOG_CATEGORY, logMethod));
                commonUtils.okResponseHandler(result, req, res, next);
            }).catch(function (err) {
                logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
                res.boom.badRequest(err);
            });
    } catch (err) {
        logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
        res.boom.badRequest(err);
    }
}

let updateOrderSequence = function (req, res, next) {
    let logMethod = "updateOrderSequence";
    logger.info(util.format("%s::%s->", LOG_CATEGORY, logMethod));
    try {
        if (!req.body) {
            throw new Error("Body parameters required.");
        }
        if (req.body && _.isEmpty(req.body)) {
            throw new Error("Request body should not be  empty ");
        }

        planService.updateOrderSequence(req.body)
            .then(function (result) {
                logger.info(util.format("<-%s::%s", LOG_CATEGORY, logMethod));
                commonUtils.okResponseHandler(result, req, res, next);
            }).catch(function (err) {
                logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, JSON.stringify(err)));
                next(err);
            });
    } catch (err) {
        logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, JSON.stringify(err)));
        next(err);
    }
}
let getOrderById = async (req, res, next) => {
    let logMethod = 'getOrderById';
    try {
        if (!req.params.orderId) {
            throw new Error("Order Id is required.");
        }
        let order_id = req.params.orderId;
        const result = await planService.getOrderById(order_id);
        logger.info(util.format("<-%s::%s", LOG_CATEGORY, logMethod));
        commonUtils.okResponseHandler(result, req, res, next);
    } catch (err) {
        logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, JSON.stringify(err)));
        res.boom.badRequest(err.message);
    }
}
////////////////// For Testing Purpose ///////////////
let test = async (req, res, next) => {
    let logMethod = 'checkFreightType';
    try {
        var manifestSourceId = (req.body.manifestSourceId) ? req.body.manifestSourceId : '';
        const result = await planService.test(manifestSourceId);
        logger.info(util.format("<-%s::%s", LOG_CATEGORY, logMethod));
        commonUtils.okResponseHandler(result, req, res, next);
    } catch (err) {
        logger.info(util.format("<-%s::%s", LOG_CATEGORY, logMethod));
        logger.error(err);
        res.boom.badRequest(err);
    }
}
// Exporting modules
module.exports = {
    unplannedOrdersList: unplannedOrdersList,
    updateOrderDetails: updateOrderDetails,
    updateOrderSequence: updateOrderSequence,
    getOrderById: getOrderById,
    CommodityWeight: CommodityWeight,
    laneDensityPalletcount: laneDensityPalletcount,
    checkFreightType: checkFreightType,
    checkLoadTypeManifest: checkLoadTypeManifest,
    removeOrderFromManifest: removeOrderFromManifest,
    test: test
}