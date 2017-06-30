'use strict';

var _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; };

var gql = require('graphql');
var execution = require('./execution');
module.exports = _extends({}, gql, {
  graphql: require('./graphql').graphql,
  execute: execution.execute,
  defaultFieldResolver: execution.defaultFieldResolver,
  responsePathAsArray: execution.responsePathAsArray
});