/*eslint camelcase:false */
'use strict';

////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2015-2016 ArangoDB GmbH, Cologne, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Alan Plum
////////////////////////////////////////////////////////////////////////////////

const _ = require('lodash');
const url = require('url');
const parseUrl = url.parse;
const formatUrl = url.format;
const parseQuery = require('querystring').parse;
const request = require('@arangodb/request');

function parse(str) {
  try {
    return JSON.parse(str);
  } catch (e) {
    if (e instanceof SyntaxError) {
      return parseQuery(str);
    }
    throw e;
  }
}

module.exports = function oauth2(cfg) {
  function getTokenRequest(code, redirect_uri) {
    const endpoint = parseUrl(cfg.tokenEndpoint);
    const body = _.extend(
      {grant_type: 'authorization_code'},
      parseQuery(endpoint.query),
      {
        client_id: cfg.clientId,
        client_secret: cfg.clientSecret,
        code
      }
    );
    if (redirect_uri) {
      body.redirect_uri = redirect_uri;
    }
    delete endpoint.search;
    delete endpoint.query;
    return {url: formatUrl(endpoint), body};
  }

  function getActiveUserUrl(access_token) {
    const endpoint = parseUrl(cfg.activeUserEndpoint);
    delete endpoint.search;
    endpoint.query = _.extend(
      parseQuery(endpoint.query),
      {access_token}
    );
    return formatUrl(endpoint);
  }

  return {
    getAuthUrl(redirect_uri, opts) {
      if (typeof redirect_uri !== 'string') {
        opts = redirect_uri;
        redirect_uri = undefined;
      }
      const endpoint = parseUrl(cfg.authEndpoint);
      delete endpoint.search;
      endpoint.query = _.extend(
        {response_type: 'code'},
        parseQuery(endpoint.query),
        opts,
        {client_id: cfg.clientId}
      );
      if (redirect_uri) {
        endpoint.query.redirect_uri = redirect_uri;
      }
      return formatUrl(endpoint);
    },
    exchangeGrantToken(code, redirect_uri) {
      const req = getTokenRequest(code, redirect_uri);
      const res = request.post(req.url, {
        headers: {accept: 'application/json'},
        form: req.body
      });
      if (!res.body) {
        throw new Error(`OAuth2 provider returned empty response with HTTP status ${res.status}`);
      }
      return parse(res.body);
    },
    fetchActiveUser(access_token) {
      if (!cfg.activeUserEndpoint) {
        return null;
      }
      const url = getActiveUserUrl({access_token});
      const res = request.get(url);
      if (!res.body) {
        throw new Error(`OAuth2 provider returned empty response with HTTP status ${res.status}`);
      }
      return parse(res.body);
    }
  };
};
