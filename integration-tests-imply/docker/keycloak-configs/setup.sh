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

/opt/jboss/keycloak/bin/kcadm.sh config credentials --server http://localhost:8080/auth --realm master --user admin --password password
/opt/jboss/keycloak/bin/kcadm.sh update realms/master -s sslRequired=NONE
/opt/jboss/keycloak/bin/kcadm.sh update realms/druid -s sslRequired=NONE
/opt/jboss/keycloak/bin/kcadm.sh create users -r druid -s username=admin -s enabled=true -s attributes='{"druid-roles" : [ "[\"admin\"]" ]}'
/opt/jboss/keycloak/bin/kcadm.sh set-password -r druid --username admin --new-password priest
/opt/jboss/keycloak/bin/kcadm.sh create users -r druid -s username=datasourceonlyuser -s enabled=true -s attributes='{"druid-roles" : [ "[\"datasourceOnlyRole\"]" ]}'
/opt/jboss/keycloak/bin/kcadm.sh set-password -r druid --username datasourceonlyuser --new-password helloworld
/opt/jboss/keycloak/bin/kcadm.sh create users -r druid -s username=datasourcewithstateuser -s enabled=true -s attributes='{"druid-roles" : [ "[\"datasourceWithStateRole\"]" ]}'
/opt/jboss/keycloak/bin/kcadm.sh set-password -r druid --username datasourcewithstateuser --new-password helloworld
/opt/jboss/keycloak/bin/kcadm.sh create users -r druid -s username=stateonlyuser -s enabled=true -s attributes='{"druid-roles" : [ "[\"stateOnlyRole\"]" ]}'
/opt/jboss/keycloak/bin/kcadm.sh set-password -r druid --username stateonlyuser --new-password helloworld
