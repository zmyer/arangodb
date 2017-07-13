'use strict';

const graphql = require('../sgq/node_modules/graphql-sync').graphql;
const sgq = require('../sgq');


let typeDefs = [`
  type BlogEntry {
    _key: String!
    authorKey: String!

    author: Author @aql(exec: "FOR author in Author filter author._key == @current.authorKey return author")
  }

  type Author {
    _key: String!
    name: String
  }

  type Query {
    blogEntry(_key: String!): BlogEntry
  }
`]

const schema = sgq(typeDefs);

const query = `
{
  blogEntry(_key: "1") {
    _key
    authorKey
    author {
      name
     }
  }
}`;

const result = graphql(schema, query);

print('-----------');
print(result);
