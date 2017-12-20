/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.campuslabs.iteration

import cats.implicits._
import cats.kernel.Semigroup

case class Node(
  id: Int,
  children: Set[Node]
) {
  override def toString: String =
    if (children.isEmpty)
      id.toString
    else
      s"$id -> ${children.mkString("[", ", ", "]")}"
}

object Node {
  private def internalCombine(x: Node, y: Node): Node =
    if (x.children.isEmpty) y
    else if (y.children.isEmpty) x
    else {
      println(s"Merging $x and $y")
      x |+| y
    }

  implicit val nodeSemigroup: Semigroup[Node] = new Semigroup[Node] {
    override def combine(x: Node, y: Node): Node = {
      if (x.id != y.id) sys.error(s"Cannot merge nodes with different IDs: $x |+| $y")

      if (x.children.isEmpty) y
      else if (y.children.isEmpty) x
      else {
        val children = (x.children ++ y.children)
          .groupBy(_.id)
          .values
          .toSet
          .map { nodes: Set[Node] => nodes.reduce(internalCombine) }
        x.copy(children = children)
      }
    }
  }
}
