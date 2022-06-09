#!/usr/bin/env node

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const fs = require('fs-extra');
const snarkdown = require('snarkdown');

const writefile = 'lib/sql-docs.js';

const MINIMUM_EXPECTED_NUMBER_OF_FUNCTIONS = 150;
const MINIMUM_EXPECTED_NUMBER_OF_DATA_TYPES = 14;

// BEGIN: Imply-added code
const IMPLY_FUNCTION_DOCS = {
  TABLE: [
    [
      'expr',
      convertMarkdownToHtml(
        'Creates an inline table object. The first argument is expected to be an `EXTERN` function.',
      ),
    ],
  ],
  EXTERN: [
    [
      'inputSource, inputFormat, rowSignature',
      convertMarkdownToHtml(
        `The EXTERN function takes three parameters:
1. \`inputSource\` - any Druid input source, as a JSON-encoded string.
2. \`inputFormat\` - any Druid input format, as a JSON-encoded string.
3. \`rowSignature\` - as a JSON-encoded array of column descriptors. Each column descriptor must have a name and a type. The type can be \`string\`, \`long\`, \`double\`, or \`float\`. This row signature will be used to map the external data into the SQL layer.`,
      ),
    ],
  ],
  JSON_VALUE: [
    [
      'expr, path',
      convertMarkdownToHtml(
        'Extract a Druid literal (`STRING`, `LONG`, `DOUBLE`) value from a `COMPLEX<json>` column or input `expr` using JSONPath syntax of `path`',
      ),
    ],
  ],
  JSON_QUERY: [
    [
      'expr, path',
      convertMarkdownToHtml(
        'Extract a `COMPLEX<json>` value from a `COMPLEX<json>` column or input `expr` using JSONPath syntax of `path`',
      ),
    ],
  ],
  JSON_OBJECT: [
    [
      'KEY expr1 VALUE expr2[, KEY expr3 VALUE expr4 ...]',
      convertMarkdownToHtml(
        'Construct a `COMPLEX<json>` storing the results of `VALUE` expressions at `KEY` expressions',
      ),
    ],
  ],
  PARSE_JSON: [
    [
      'expr',
      convertMarkdownToHtml(
        'Deserialize a JSON `STRING` into a `COMPLEX<json>` to be used with expressions which operate on `COMPLEX<json>` inputs.',
      ),
    ],
  ],
  TO_JSON: [
    [
      'expr',
      convertMarkdownToHtml(
        'Convert any input type to `COMPLEX<json>` to be used with expressions which operate on `COMPLEX<json>` inputs, like a `CAST` operation (rather than deserializing `STRING` values like `PARSE_JSON`)',
      ),
    ],
  ],
  TO_JSON_STRING: [
    ['expr', convertMarkdownToHtml('Convert a `COMPLEX<json>` input into a JSON `STRING` value')],
  ],
  JSON_KEYS: [
    [
      'expr, path',
      convertMarkdownToHtml(
        'Get array of field names in `expr` at the specified JSONPath `path`, or null if the data does not exist or have any fields',
      ),
    ],
  ],
  JSON_PATHS: [
    ['expr', convertMarkdownToHtml('Get array of all JSONPath paths available in `expr`')],
  ],
};
// END: Imply-modified code

function hasHtmlTags(str) {
  return /<(a|br|span|div|p|code)\/?>/.test(str);
}

function sanitizeArguments(str) {
  str = str.replace(/`<code>&#124;<\/code>`/g, '|'); // convert the hack to get | in a table to a normal pipe

  // Ensure there are no more html tags other than the <code> we just removed
  if (hasHtmlTags(str)) {
    throw new Error(`Arguments contain HTML: ${str}`);
  }

  return str;
}

function convertMarkdownToHtml(markdown) {
  markdown = markdown.replace(/<br\/?>/g, '\n'); // Convert inline <br> to newlines

  // Ensure there are no more html tags other than the <br> we just removed
  if (hasHtmlTags(markdown)) {
    throw new Error(`Markdown contains HTML: ${markdown}`);
  }

  // Concert to markdown
  markdown = snarkdown(markdown);

  return markdown
    .replace(/<br \/>/g, '<br /><br />') // Double up the <br>s
    .replace(/<a[^>]*>(.*?)<\/a>/g, '$1'); // Remove links
}

const readDoc = async () => {
  const data = [
    await fs.readFile('../docs/querying/sql-data-types.md', 'utf-8'),
    await fs.readFile('../docs/querying/sql-scalar.md', 'utf-8'),
    await fs.readFile('../docs/querying/sql-aggregations.md', 'utf-8'),
    await fs.readFile('../docs/querying/sql-multivalue-string-functions.md', 'utf-8'),
    await fs.readFile('../docs/querying/sql-operators.md', 'utf-8'),
  ].join('\n');

  const lines = data.split('\n');

  const functionDocs = {};
  const dataTypeDocs = [];
  for (let line of lines) {
    const functionMatch = line.match(/^\|\s*`(\w+)\(([^|]*)\)`\s*\|([^|]+)\|(?:([^|]+)\|)?$/);
    if (functionMatch) {
      const functionName = functionMatch[1];
      const args = sanitizeArguments(functionMatch[2]);
      const description = convertMarkdownToHtml(functionMatch[3]);

      functionDocs[functionName] = functionDocs[functionName] || [];
      functionDocs[functionName].push([args, description]);
    }

    const dataTypeMatch = line.match(/^\|([A-Z]+)\|([A-Z]+)\|([^|]*)\|([^|]*)\|$/);
    if (dataTypeMatch) {
      dataTypeDocs.push([
        dataTypeMatch[1],
        dataTypeMatch[2],
        convertMarkdownToHtml(dataTypeMatch[4]),
      ]);
    }
  }

  // Make sure there are enough functions found
  const numFunction = Object.keys(functionDocs).length;
  if (numFunction < MINIMUM_EXPECTED_NUMBER_OF_FUNCTIONS) {
    throw new Error(
      `Did not find enough function entries did the structure of '${readfile}' change? (found ${numFunction} but expected at least ${MINIMUM_EXPECTED_NUMBER_OF_FUNCTIONS})`,
    );
  }

  // Make sure there are at least 10 data types for sanity
  const numDataTypes = dataTypeDocs.length;
  if (numDataTypes < MINIMUM_EXPECTED_NUMBER_OF_DATA_TYPES) {
    throw new Error(
      `Did not find enough data type entries did the structure of '${readfile}' change? (found ${numDataTypes} but expected at least ${MINIMUM_EXPECTED_NUMBER_OF_DATA_TYPES})`,
    );
  }

  // BEGIN: Imply-added code for Talaria execution
  console.log(`Adding ${Object.keys(IMPLY_FUNCTION_DOCS).length} Imply only functions`);
  Object.assign(functionDocs, IMPLY_FUNCTION_DOCS);
  // END: Imply-modified code for Talaria execution

  const content = `/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// This file is auto generated and should not be modified

// prettier-ignore
exports.SQL_DATA_TYPES = ${JSON.stringify(dataTypeDocs, null, 2)};

// prettier-ignore
exports.SQL_FUNCTIONS = ${JSON.stringify(functionDocs, null, 2)};
`;

  console.log(`Found ${numDataTypes} data types and ${numFunction} functions`);
  await fs.writeFile(writefile, content, 'utf-8');
};

readDoc();
