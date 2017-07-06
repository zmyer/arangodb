'use strict';

const gql = require('graphql-sync');
const graphql = gql.graphql;

const generator = require('./graphql-generator');



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

let schema = generator(typeDefs);

let query = `
{
  blogEntry(_key: "1") {
    _key
    authorKey
    author {
      name
     }
  }
}`;

let result = graphql(schema, query);

print('-----------');
print(result);
