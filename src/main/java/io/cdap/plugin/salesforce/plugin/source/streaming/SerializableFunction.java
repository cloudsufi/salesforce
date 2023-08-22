/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.salesforce.plugin.source.streaming;

import scala.Function1;

import java.io.Serializable;
import java.util.function.Function;

/**
 * Interface used to make a Java.util.function.Function serializable.
 * @param <T> the type of the input to the function
 * @param <R> the type of the result of the function
 */
public interface SerializableFunction<T, R> extends Function<T, R>, Function1<T, R>, Serializable {
}
