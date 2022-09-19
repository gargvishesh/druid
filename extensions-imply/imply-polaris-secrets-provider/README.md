<!--
  ~ Copyright (c) Imply Data, Inc. All rights reserved.
  ~
  ~ This software is the confidential and proprietary information
  ~ of Imply Data, Inc. You shall not disclose such Confidential
  ~ Information and shall use it only in accordance with the terms
  ~ of the license agreement you entered into with Imply.
  -->

# Imply Polaris Secrets Provider

This extension provides implementations for various interfaces that are used to provide config properties with secret values.

* Adds a KafkaConfigOverrides implementation, `ImplyPolarisSecretsConfluentConfigOverrides` that reads from AWS Secrets Manager, used to retrieve secret connection properties for ingestion from Confluent Cloud streams.

## To load the extension

Add `imply-polaris-secrets-provider` to the extensions load list

```json
druid.extensions.loadList=["druid-lookups-cached-global","druid-datasketches","imply-polaris-secrets-provider"]
...
```

## Using the Polaris Secrets Provider

### ImplyPolarisSecretsConfluentConfigOverrides

In the IOConfig of a Kafka Supervisor spec, set the `configOverrides` property to the following:
```json
{
  "type": "polarisSecretsConfluent",
  "secretType": "confluent",
  "secretKey": "polaris-connection-secret-<customerId>-<connectionName"
}
```
- `secretType`: Should always be `confluent` for now, used to determine how to deserialize the secret value object (these do not have a `type` field).
- `secretKey`: The ID of the secret in the AWS Secrets Manager that will be used for secret lookup.

The Polaris tables service will set this in the Kafka Supervisor specs that it creates for pull ingestion from Confluent Cloud.

### Configuration

The secrets provider has some optional configuration properties useful for testing purposes, allowing the user to directly specify the AWS region and access key pair that should be used when retrieving secrets.

In a production environment, these configuration properties should not be set, in which case the secrets provider will use the default AWS region provider and credential provider chains.

#### In common.runtime.properties:
| property | description | required? | default |
| --- | --- | --- | --- |
| `imply.polaris.secrets.region` | AWS region to use  | no | None. |
| `imply.polaris.secrets.testingAccessKey` | AWS access key for testing purposes  | no | None. |
| `imply.polaris.secrets.testingSecretKey` | AWS secret key for testing purposes  | no | None. |
