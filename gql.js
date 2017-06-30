'use strict';

const gql = require('graphql-sync');
const lodash_1 = require("lodash");

const GraphQLSchema = gql.GraphQLSchema;
if (!GraphQLSchema) console.log('!GraphQLSchema');
const GraphQLObjectType = gql.GraphQLObjectType;
if (!GraphQLObjectType) console.log('!GraphQLObjectType');
const GraphQLString = gql.GraphQLString;
if (!GraphQLString) console.log('!GraphQLString');
const GraphQLNonNull = gql.GraphQLNonNull;
if (!GraphQLNonNull) console.log('!GraphQLNonNull');
const GraphQLUnionType = gql.GraphQLUnionType;
if (!GraphQLUnionType) console.log('!GraphQLUnionType');
const GraphQLList = gql.GraphQLList;
if (!GraphQLList) console.log('!GraphQLList');
const GraphQLInterfaceType = gql.GraphQLInterfaceType;
if (!GraphQLInterfaceType) console.log('!GraphQLInterfaceType');
const GraphQLID = gql.GraphQLID;
if (!GraphQLID) console.log('!GraphQLID');
const GraphQLBoolean = gql.GraphQLBoolean;
if (!GraphQLBoolean) console.log('!GraphQLBoolean');
const DirectiveLocation = gql.DirectiveLocation;
if (!DirectiveLocation) console.log('!DirectiveLocation');
const GraphQLDirective = gql.GraphQLDirective;
if (!GraphQLDirective) console.log('!GraphQLDirective');
const GraphQLIncludeDirective = gql.GraphQLIncludeDirective;
if (!GraphQLIncludeDirective) console.log('!GraphQLIncludeDirective');
const GraphQLSkipDirective = gql.GraphQLSkipDirective;
if (!GraphQLSkipDirective) console.log('!GraphQLSkipDirective');
const graphql = gql.graphql;
if (!graphql) console.log('!graphql');

const buildASTSchema = gql.buildASTSchema;
if (!buildASTSchema) console.log('!buildASTSchema');
const extendSchema = gql.extendSchema;
if (!extendSchema) console.log('!extendSchema');

const parse = gql.parse;
if (!parse) console.log('!parse');

const graphql_1 = gql;
const graphql_3 = gql;

/*
const resolveWithInstrumentation = (resolve) => {
  return (source, args, context, info) => {
    console.log('resolveWithInstrumentation()');
    const directives = info.fieldASTs[0].directives;
    const instrumentDirective = directives.filter(d => d.name.value === InstrumentDirective.name)[0];
    if (!instrumentDirective) {
      return resolve(source, args, context, info);
    }

    const start = new Date();

    console.log('bevore resolve');
    print(source);
    print(args);
    const result = resolve(source, args, context, info);

    console.log('result is');
    console.log(result);
    const diff = (new Date() - start);
    const tag = instrumentDirective.arguments[0].value.value;
    console.log(`Instrumented ${tag} @ ${diff}ms`);
    
    return result;
  };
};

const ResultType = new GraphQLObjectType({
  name : 'Result',
  fields : {
    text : {
      type : new GraphQLNonNull(GraphQLString)
    },
    id : {
      type : new GraphQLNonNull(GraphQLID),
      resolve: resolveWithInstrumentation((result) => {
        return result.id;
      })
    }
  }
});

const InstrumentDirective = new GraphQLDirective({
  name: 'aql',
  description:
    'Instrument the time it takes to resolve the field',
  locations: [
    DirectiveLocation.FIELD,
  ],
  args: {
    query: {
      type: new GraphQLNonNull(GraphQLString),
      description: 'A tag to store in the metrics store'
    }
  },
});

const schema = new GraphQLSchema({
  directives: [
    InstrumentDirective,
    GraphQLIncludeDirective,
    GraphQLSkipDirective
  ],
  query: new GraphQLObjectType({
    name: 'RootQueryType',
    fields: {
        search: {
            type: ResultType,
            args:  {
                text: { type : new GraphQLNonNull(GraphQLString) }
            },
            resolve(root, args) {
                console.log('resolve in schema');
                const text = args.text;
                return { id: `id:${text}` };
            }
        }
    }
  })
});

let query = `
  {
    search(text: "cat") {
      id @aql(query: "search.id")
    }
  }
`;

let result = graphql(schema, query);

print(result);


print('-----------');

*/


function makeExecutableSchema(
  typeDefs,
  resolvers = {},
  connectors,
  logger,
  allowUndefinedInResolve = true,
  resolverValidationOptions = {}) {
  const jsSchema = _generateSchema(typeDefs, resolvers, logger, allowUndefinedInResolve, resolverValidationOptions);
  return jsSchema;
}

function _generateSchema(
  typeDefinitions,
  resolveFunctions,
  logger,
  // TODO: rename to allowUndefinedInResolve to be consistent
  allowUndefinedInResolve,
  resolverValidationOptions
) {
  if (typeof resolverValidationOptions !== 'object') {
    throw new Error('Expected `resolverValidationOptions` to be an object');
  }
  if (!typeDefinitions) {
    throw new Error('Must provide typeDefs');
  }
  if (!resolveFunctions) {
    throw new Error('Must provide resolvers');
  }

  // TODO: check that typeDefinitions is either string or array of strings

  const [ast, schema] = buildSchemaFromTypeDefinitions(typeDefinitions);

  // print(JSON.stringify(ast, false, 2));

  ast.definitions.forEach( definition => {
    const x = JSON.parse(JSON.stringify(definition, false, 2));

    if (x.name.value == 'Query') {
      console.log('found query');
      const field = x.fields[0];
      const fieldName = field.name.value;

      print(fieldName);

    

      const collection = field.type.name.value;
      const args = field.arguments;

      print('collection is', collection);

      const argList = [];

      for (const arg of args) {
        print(arg);
        print('arg.name.value');
        print(arg.name.value);
        print('arg.type.type.name.value');
        print(arg.type.type.name.value);

        argList.push({[arg.name.value]: arg.type.type.name.value});
      } // for



      // print(collection);
      // print(argList);

      resolveFunctions['Query'] = {};
      resolveFunctions['Query'][fieldName] = function(dontknow, args, dontknow2, astPart) { // a undefined, b id:4, undefined, astpart

          // print(args);

          const filterList = Object.keys(args).map(arg => `doc.${arg} == ${'string' === typeof args[arg] ? "'" + args[arg] + "'" : args[arg] }`);

          print('filterList', filterList);

          const res = db._query(`for doc in ${collection} filter ${filterList.join(' AND ')} return doc`).toArray();


          return res.pop();

      }
    } // if
  }); // forEach

  addResolveFunctionsToSchema(schema, resolveFunctions);

  assertResolveFunctionsPresent(schema, resolverValidationOptions);

  if (!allowUndefinedInResolve) {
    addCatchUndefinedToSchema(schema);
  }

  if (logger) {
    addErrorLoggingToSchema(schema, logger);
  }

  return schema;
}

function buildSchemaFromTypeDefinitions(typeDefinitions) {
  // TODO: accept only array here, otherwise interfaces get confusing.
  let myDefinitions = typeDefinitions;
  let astDocument;

  if (isDocumentNode(typeDefinitions)) {
    astDocument = typeDefinitions;
  } else if (typeof myDefinitions !== 'string') {
    if (!Array.isArray(myDefinitions)) {
      const type = typeof myDefinitions;
      throw new Error(`typeDefs must be a string, array or schema AST, got ${type}`);
    }
    myDefinitions = concatenateTypeDefs(myDefinitions);
  }

  if (typeof myDefinitions === 'string') {
    astDocument = parse(myDefinitions);
  }

  let schema = buildASTSchema(astDocument);

  const extensionsAst = extractExtensionDefinitions(astDocument);
  if (extensionsAst.definitions.length > 0) {
    schema = extendSchema(schema, extensionsAst);
  }

  return [astDocument, schema];
}

function isDocumentNode(typeDefinitions) {
  return typeDefinitions.kind !== undefined;
}

function concatenateTypeDefs(typeDefinitionsAry, calledFunctionRefs) {
    if (calledFunctionRefs === void 0) { calledFunctionRefs = []; }
    var resolvedTypeDefinitions = [];
    typeDefinitionsAry.forEach(function (typeDef) {
        if (isDocumentNode(typeDef)) {
            typeDef = graphql_1.print(typeDef);
        }
        if (typeof typeDef === 'function') {
            if (calledFunctionRefs.indexOf(typeDef) === -1) {
                calledFunctionRefs.push(typeDef);
                resolvedTypeDefinitions = resolvedTypeDefinitions.concat(concatenateTypeDefs(typeDef(), calledFunctionRefs));
            }
        }
        else if (typeof typeDef === 'string') {
            resolvedTypeDefinitions.push(typeDef.trim());
        }
        else {
            var type = typeof typeDef;
            throw new SchemaError("typeDef array must contain only strings and functions, got " + type);
        }
    });
    return lodash_1.uniq(resolvedTypeDefinitions.map(function (x) { return x.trim(); })).join('\n');
}

function extractExtensionDefinitions(ast) {
    var extensionDefs = ast.definitions.filter(function (def) { return def.kind === graphql_1.Kind.TYPE_EXTENSION_DEFINITION; });
    return Object.assign({}, ast, {
        definitions: extensionDefs,
    });
}

function addResolveFunctionsToSchema(schema, resolveFunctions) {
    Object.keys(resolveFunctions).forEach(function (typeName) {
        var type = schema.getType(typeName);
        if (!type && typeName !== '__schema') {
            throw new Error("\"" + typeName + "\" defined in resolvers, but not in schema");
        }
        Object.keys(resolveFunctions[typeName]).forEach(function (fieldName) {
            if (fieldName.startsWith('__')) {
                // this is for isTypeOf and resolveType and all the other stuff.
                // TODO require resolveType for unions and interfaces.
                type[fieldName.substring(2)] = resolveFunctions[typeName][fieldName];
                return;
            }
            if (type instanceof graphql_3.GraphQLScalarType) {
                type[fieldName] = resolveFunctions[typeName][fieldName];
                return;
            }
            var fields = getFieldsForType(type);
            if (!fields) {
                throw new Error(typeName + " was defined in resolvers, but it's not an object");
            }
            if (!fields[fieldName]) {
                throw new Error(typeName + "." + fieldName + " defined in resolvers, but not in schema");
            }
            var field = fields[fieldName];
            var fieldResolve = resolveFunctions[typeName][fieldName];
            if (typeof fieldResolve === 'function') {
                // for convenience. Allows shorter syntax in resolver definition file
                setFieldProperties(field, { resolve: fieldResolve });
            }
            else {
                if (typeof fieldResolve !== 'object') {
                    throw new SchemaError("Resolver " + typeName + "." + fieldName + " must be object or function");
                }
                setFieldProperties(field, fieldResolve);
            }
        });
    });
}

function setFieldProperties(field, propertiesObj) {
    Object.keys(propertiesObj).forEach(function (propertyName) {
        field[propertyName] = propertiesObj[propertyName];
    });
}


function getFieldsForType(type) {
    if ((type instanceof graphql_3.GraphQLObjectType) ||
        (type instanceof graphql_3.GraphQLInterfaceType)) {
        return type.getFields();
    }
    else {
        return undefined;
    }
}

function assertResolveFunctionsPresent(schema, resolverValidationOptions) {
    if (resolverValidationOptions === void 0) { resolverValidationOptions = {}; }
    var _a = resolverValidationOptions.requireResolversForArgs, requireResolversForArgs = _a === void 0 ? false : _a, _b = resolverValidationOptions.requireResolversForNonScalar, requireResolversForNonScalar = _b === void 0 ? false : _b, _c = resolverValidationOptions.requireResolversForAllFields, requireResolversForAllFields = _c === void 0 ? false : _c;
    if (requireResolversForAllFields && (requireResolversForArgs || requireResolversForNonScalar)) {
        throw new TypeError('requireResolversForAllFields takes precedence over the more specific assertions. ' +
            'Please configure either requireResolversForAllFields or requireResolversForArgs / ' +
            'requireResolversForNonScalar, but not a combination of them.');
    }
    forEachField(schema, function (field, typeName, fieldName) {
        // requires a resolve function for *every* field.
        if (requireResolversForAllFields) {
            expectResolveFunction(field, typeName, fieldName);
        }
        // requires a resolve function on every field that has arguments
        if (requireResolversForArgs && field.args.length > 0) {
            expectResolveFunction(field, typeName, fieldName);
        }
        // requires a resolve function on every field that returns a non-scalar type
        if (requireResolversForNonScalar && !(graphql_3.getNamedType(field.type) instanceof graphql_3.GraphQLScalarType)) {
            expectResolveFunction(field, typeName, fieldName);
        }
    });
}

function forEachField(schema, fn) {
    var typeMap = schema.getTypeMap();
    Object.keys(typeMap).forEach(function (typeName) {
        var type = typeMap[typeName];
        // TODO: maybe have an option to include these?
        if (!graphql_3.getNamedType(type).name.startsWith('__') && type instanceof graphql_3.GraphQLObjectType) {
            var fields_1 = type.getFields();
            Object.keys(fields_1).forEach(function (fieldName) {
                var field = fields_1[fieldName];
                fn(field, typeName, fieldName);
            });
        }
    });
}

/*

const jsSchema = makeExecutableSchema({
  typeDefs,
  resolvers,
  logger, // optional
  allowUndefinedInResolve = false, // optional
  resolverValidationOptions = {}, // optional
});
*/




let typeDefs = [`
  type Author {
    id: Int!
    firstName: String
    lastName: String
    name: String
  }

  type Query {
    author(id: Int!, name: String!): Author
  }
`]

let schema = makeExecutableSchema(typeDefs);

let query = `
{
  author(id: 4, name: "Manuel") {
    firstName
    name
    id
  }
}
`;

let result = graphql(schema, query);

print('-----------');
print(result);


