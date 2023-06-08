/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.lookup;

import com.google.inject.BindingAnnotation;
import org.apache.druid.guice.annotations.Parent;
import org.apache.druid.guice.annotations.PublicApi;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Binding annotation for org.apache.druid.server.DruidNode.
 * Indicates that the DruidNode bound with this annotation holds the information of the machine where this process
 * is running.
 *
 * @see Parent
 */
@Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@BindingAnnotation
@PublicApi
public @interface ImplyLookup
{
}
