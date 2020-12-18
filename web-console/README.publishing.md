<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

# Publishing private npm packages

The web console is published as a private package called `@implydata/web-console` on npm.

## How it works

Normally, publishing an npm package is fairly straightforward. But there are a few constraints that make it more complicated for the web console:

- The public project is only distributed as part of Druid. It is not intended to be published as a standalone project or used by other (public) projects directly.
- The private project is virtually identical to the public one and changes are merged downstream by an automated process. Anything that introduces conflicts in this process would add significant friction for the team.
- The version is synchronized with the Druid version, but we may want to publish more frequently than Druid releases.

By only adding new files relative to the public version, this ensures that public repo knows nothing about publishing and anything related to publishing in the private repo will not cause merge conflicts.
Versions are explained below:

## Versions

Versions of `@implydata/web-console` use the following format:

```
{DRUID_VERSION}-{PRIVATE_VERSION_COUNTER}
```

For example, the Druid version `0.30.1` might have the following web-console versions:

- `0.30.1-0`
- `0.30.1-1`
- `0.30.1-2`

When the Druid version is bumped to `0.31.0`, the next web-console version would be:

- `0.31.0-0`

These versions are determined automatically by the publishing process.

## How to publish

You will need publishing rights in the `@implydata` npm org. Then in the `web-console` project:

```sh
npm install
./script/build-dist
./script/publish
```

That is all.
