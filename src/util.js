const
    MODULE_NAME = 'module.persistence.mongodb';

exports._assert = function(value, errMsg = 'undefined error', errType = Error) {
    if (!value) {
        const err = new errType(`${MODULE_NAME} : ${errMsg}`);
        Error.captureStackTrace(err, exports._assert);
        throw err;
    }
};

exports._lockProp = function(obj, ...keys) {
    const lock = { writable: false, configurable: false };
    for (let key of keys) {
        Object.defineProperty(obj, key, lock);
    }
};

exports._strValidator = function(pattern) {
    return value => pattern.test(value);
};

exports._isString = function(value) {
    return typeof value === 'string';
};

exports._isObject = function(value) {
    return value && typeof value === 'object';
};