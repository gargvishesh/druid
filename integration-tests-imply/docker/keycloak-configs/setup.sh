#!/bin/bash

#
# Copyright (c) Imply Data, Inc. All rights reserved.
#
# This software is the confidential and proprietary information
# of Imply Data, Inc. You shall not disclose such Confidential
# Information and shall use it only in accordance with the terms
# of the license agreement you entered into with Imply.
#

set -e

# start session
/opt/jboss/keycloak/bin/kcadm.sh config credentials --server http://localhost:8080/auth --realm master --user admin --password password

# disable ssl so debugging is less painful
/opt/jboss/keycloak/bin/kcadm.sh update realms/master -s sslRequired=NONE
/opt/jboss/keycloak/bin/kcadm.sh update realms/druid -s sslRequired=NONE

# set some-druid-cluster secret
/opt/jboss/keycloak/bin/kcadm.sh update clients/cbd9eb75-1a7c-4d95-8fbc-36adb32a1fdc -s "secret=druid-user-secret" -r druid
# set some-druid-cluster-internal secret
/opt/jboss/keycloak/bin/kcadm.sh update clients/f3f0e4e4-1844-401b-934c-30f66c07d45f -s "secret=secret" -r druid

# add users and some-druid-cluster roles to users
/opt/jboss/keycloak/bin/kcadm.sh create users -r druid -s username=admin -s enabled=true
/opt/jboss/keycloak/bin/kcadm.sh set-password -r druid --username admin --new-password priest
/opt/jboss/keycloak/bin/kcadm.sh add-roles -r druid --uusername admin --cclientid some-druid-cluster --rolename admin

/opt/jboss/keycloak/bin/kcadm.sh create users -r druid -s username=datasourceonlyuser -s enabled=true
/opt/jboss/keycloak/bin/kcadm.sh set-password -r druid --username datasourceonlyuser --new-password helloworld
/opt/jboss/keycloak/bin/kcadm.sh add-roles -r druid --uusername datasourceonlyuser --cclientid some-druid-cluster --rolename datasourceOnlyRole

/opt/jboss/keycloak/bin/kcadm.sh create users -r druid -s username=datasourcewithsysuser -s enabled=true
/opt/jboss/keycloak/bin/kcadm.sh set-password -r druid --username datasourcewithsysuser --new-password helloworld
/opt/jboss/keycloak/bin/kcadm.sh add-roles -r druid --uusername datasourcewithsysuser --cclientid some-druid-cluster --rolename datasourceWithSysRole

/opt/jboss/keycloak/bin/kcadm.sh create users -r druid -s username=datasourcewithstateuser -s enabled=true
/opt/jboss/keycloak/bin/kcadm.sh set-password -r druid --username datasourcewithstateuser --new-password helloworld
/opt/jboss/keycloak/bin/kcadm.sh add-roles -r druid --uusername datasourcewithstateuser --cclientid some-druid-cluster --rolename datasourceWithStateRole

/opt/jboss/keycloak/bin/kcadm.sh create users -r druid -s username=stateonlyuser -s enabled=true
/opt/jboss/keycloak/bin/kcadm.sh set-password -r druid --username stateonlyuser --new-password helloworld
/opt/jboss/keycloak/bin/kcadm.sh add-roles -r druid --uusername stateonlyuser --cclientid some-druid-cluster --rolename stateOnlyRole

curl https://repo.qa.imply.io:443/artifactory/tgz-local/keycloak-not-before-policies-api/keycloak-not-before-policies-api-$KEYCLOAK_NOT_BEFORE_API_VERSION.tar.gz -o /tmp/keycloak-not-before-policies-api-$KEYCLOAK_NOT_BEFORE_API_VERSION.tar.gz
tar -xzvf /tmp/keycloak-not-before-policies-api-$KEYCLOAK_NOT_BEFORE_API_VERSION.tar.gz -C /tmp
cp /tmp/keycloak-not-before-policies-api-$KEYCLOAK_NOT_BEFORE_API_VERSION.jar /opt/jboss/keycloak/standalone/deployments
