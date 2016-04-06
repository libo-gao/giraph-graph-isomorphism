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

package org.apache.giraph.examples;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Set;

import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.google.common.collect.ImmutableSet;

public class queryGraphVertex{
	private Set<Long> inNode;
	private Set<Long> outNode;
	queryGraphVertex(){};
	//todo
	void build(){};
}

/**
 * Worker context for graph isomorphism.
 */
public class GraphIsomorphismWorkerContext extends WorkerContext {

  private Set<queryGraphVertex> queryGraph;
  
  /** Logger */
  private static final Logger LOG = Logger
      .getLogger(GraphIsomorphismWorkerContext.class);


  /**
   * load the query graph from input file
   * @param configuration The configuration.
   * @return a (possibly empty) set of source vertices
   */
  private ImmutableSet<Long> loadGraph(Configuration configuration) {
    ImmutableSet.Builder<Long> builder = ImmutableSet.builder();
    long sourceVertex = configuration.getLong(SOURCE_VERTEX, Long.MIN_VALUE);
    if (sourceVertex != Long.MIN_VALUE) {
      return ImmutableSet.of(sourceVertex);
    } else {
      Path sourceFile = null;
      try {

        Path[] cacheFiles = DistributedCache.getLocalCacheFiles(configuration);
        if (cacheFiles == null || cacheFiles.length == 0) {
          // empty set if no source vertices configured
          return ImmutableSet.of();
        }

        sourceFile = cacheFiles[0];
        FileSystem fs = FileSystem.getLocal(configuration);
        BufferedReader in = new BufferedReader(new InputStreamReader(
            fs.open(sourceFile), Charset.defaultCharset()));
        String line;
        while ((line = in.readLine()) != null) {
          builder.add(Long.parseLong(line));
        }
        in.close();
      } catch (IOException e) {
        getContext().setStatus(
            "Could not load local cache files: " + sourceFile);
        LOG.error("Could not load local cache files: " + sourceFile, e);
      }
    }
    return builder.build();
  }

  /**
   * build the query graph by loading the graph and 
   *
   * @param configuration the conf
   */
  private void buildQueryGraph(Configuration configuration) {
	  queryGraph = loadGraph(configuration);
	  //todo
	  queryGraph.build();
  }
  
  @Override
  public void preApplication() throws InstantiationException,
      IllegalAccessException {
    buildQueryGraph(getContext().getConfiguration());
  }

  @Override
  public void preSuperstep() {
  }

  @Override
  public void postSuperstep() {
  }

  @Override
  public void postApplication() {
  }
}
