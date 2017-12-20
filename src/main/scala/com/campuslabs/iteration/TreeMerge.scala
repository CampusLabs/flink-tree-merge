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

import java.lang

import cats.implicits._
import org.apache.flink.api.common.functions.{CoGroupFunction, JoinFunction}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.extensions._
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._

object TreeMerge {
  val treeData: Seq[(Int, Int)] =
    (0 until 10).map(x => x + 1 -> x) ++
      Seq(100 -> 42, 101 -> 42, 102 -> 42, 103 -> 102, 104 -> 103, 105 -> 103, 106 -> 105)

  type WrappedNode = (Node, Int)

  class JoinParentChild extends CoGroupFunction[WrappedNode, WrappedNode, Either[WrappedNode, WrappedNode]] {
    override def coGroup(
      wrappedChildren: lang.Iterable[WrappedNode],
      wrappedParents: lang.Iterable[WrappedNode],
      out: Collector[Either[WrappedNode, WrappedNode]]
    ): Unit = {
      val children = wrappedChildren.map(_._1)
      val parents = wrappedParents.map(_._1)

      val maybeChild = children.reduceOption(_ |+| _)
      val maybeParent = parents.headOption

      val merged = for {parent <- maybeParent; child <- maybeChild} yield {
        val newParent = parent.copy(children = Set(child))
        if (parent.children.isEmpty)
          newParent
        else
          parent |+| newParent
      }

      val result = merged.orElse(maybeChild)
      result.foreach { node =>
        if (maybeParent.isEmpty)
          out.collect(Left(node -> node.id))
        else
          node.children.foreach { child =>
            out.collect(Right(node -> child.id))
          }
      }
    }
  }

  class JoinRoot extends JoinFunction[WrappedNode, WrappedNode, WrappedNode] {
    override def join(x: WrappedNode, y: WrappedNode): WrappedNode = (x, y) match {
      case (t, null) => t
      case (null, t) => t
      case ((n1, i), (n2, _)) => (n1 |+| n2) -> i
    }
  }

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val nodes = env.fromCollection(treeData).mapWith {
      case (k, v) => Node(v, Set(Node(k, Set.empty)))
    }

    val initialWorkSet = nodes.flatMap { node =>
      node.children.map(node -> _.id)
    }

    val initialSolutionSet = env.fromCollection(List[WrappedNode]())
    val maxIterations = 1000
    val result = initialSolutionSet.iterateDelta(initialWorkSet, maxIterations, Array(1)) { (solutionSet, workSet) =>
      val joined = workSet.coGroup(workSet).where(_._1.id).equalTo(_._2).apply(new JoinParentChild)
      val newSolutionSet = joined.flatMap(_.left.toOption)
      val newWorkingSet = joined.flatMap(_.right.toOption)
      val deltas = newSolutionSet.join(solutionSet).where(1).equalTo(1).apply(new JoinRoot)

      (deltas, newWorkingSet)
    }

    result
      .map(_._1)
      .setParallelism(1)
      .sortPartition(_.id, Order.ASCENDING)
      .print()
  }
}
