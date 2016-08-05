/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation.reference.sys.node.local;

import io.crate.metadata.SimpleObjectExpression;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.operation.reference.NestedObjectExpression;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Constants;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.monitor.os.OsInfo;

class NodeOsInfoExpression extends NestedObjectExpression {

    private static final String AVAILABLE_PROCESSORS = "available_processors";
    private static final String OS = "name";
    private static final String ARCH = "arch";
    private static final String VERSION = "version";

    private static final SimpleObjectExpression OS_NAME_EXP = new SimpleObjectExpression<BytesRef>() {
        @Override
        public BytesRef value() {
            return BytesRefs.toBytesRef(Constants.OS_NAME);
        }
    };
    private static final SimpleObjectExpression OS_ARCH_EXP = new SimpleObjectExpression<BytesRef>() {
        @Override
        public BytesRef value() {
            return BytesRefs.toBytesRef(Constants.OS_ARCH);
        }
    };

    private static final SimpleObjectExpression OS_VERSION_EXP = new SimpleObjectExpression<BytesRef>() {
        @Override
        public BytesRef value() {
            return BytesRefs.toBytesRef(Constants.OS_VERSION);
        }
    };

    NodeOsInfoExpression(OsInfo info) {
        addChildImplementations(info);
    }

    private void addChildImplementations(final OsInfo info) {
        childImplementations.put(AVAILABLE_PROCESSORS, new SimpleObjectExpression<Integer>() {
            @Override
            public Integer value() {
                return info.getAvailableProcessors();
            }
        });
        childImplementations.put(OS, OS_NAME_EXP);
        childImplementations.put(ARCH, OS_ARCH_EXP);
        childImplementations.put(VERSION, OS_VERSION_EXP);
        childImplementations.put(SysNodesTableInfo.Columns.OS_INFO_JVM.name(), new NodeOsJvmExpression());
    }
}
