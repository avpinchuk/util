/*
 * Copyright (c) 2021, Alexander Pinchuk
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.github.avpinchuk.util;

import java.util.Arrays;

/**
 * Utility class to get a caller info.
 *
 * <p>StackTraceElement[] returned by Thread.currentThread().getStackTrace()
 * contains follow frames:
 *
 * [0] Thread.getStackTrace() frame
 * [1] One of getXyzMethod() frames
 * [2] Enclosing call stack frame
 * [3] First (direct) call stack frame
 * and so on
 */
public final class CallerInfo {

    private static final int DIRECT_CALLER_INDEX = 3;

    public static final class MethodDescriptor {
        private final String className;
        private final String methodName;

        public MethodDescriptor(String className, String methodName) {
            this.className = className;
            this.methodName = methodName;
        }

        public String getClassName() {
            return className;
        }

        public String getMethodName() {
            return methodName;
        }
    }

    /**
     * [0] CallerInfo$ClassContext
     * [1] CallerInfo
     * [2] Enclosing class
     * [3] First (direct) caller class
     * ...
     */
    private static final class ClassContext extends SecurityManager {
        private static final ClassContext INSTANCE = new ClassContext();

        public Class<?>[] getCallerClassChain() {
            return getClassContext();
        }
    }

    private CallerInfo() {
        throw new AssertionError();
    }

    public static MethodDescriptor getEnclosingMethod() {
        StackTraceElement stackFrame = Thread.currentThread().getStackTrace()[2];
        return new MethodDescriptor(stackFrame.getClassName(), stackFrame.getMethodName());
    }

    public static MethodDescriptor getDirectCallerMethod() {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        MethodDescriptor desc = null;
        if (stackTrace.length > DIRECT_CALLER_INDEX) {
            StackTraceElement stackFrame = stackTrace[DIRECT_CALLER_INDEX];
            desc = new MethodDescriptor(stackFrame.getClassName(), stackFrame.getMethodName());
        }
        return desc;
    }

    public static MethodDescriptor getCallerMethod(int level) {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        MethodDescriptor desc = null;
        if (stackTrace.length > level + DIRECT_CALLER_INDEX) {
            StackTraceElement stackFrame = stackTrace[level + DIRECT_CALLER_INDEX];
            desc = new MethodDescriptor(stackFrame.getClassName(), stackFrame.getMethodName());
        }
        return desc;
    }

    public static MethodDescriptor[] getCallerMethodChain() {
        return Arrays.stream(Thread.currentThread().getStackTrace())
                .skip(DIRECT_CALLER_INDEX)
                .map(frame -> new MethodDescriptor(frame.getClassName(), frame.getMethodName()))
                .toArray(MethodDescriptor[]::new);
    }

    public static Class<?> getEnclosingClass() {
        return ClassContext.INSTANCE.getCallerClassChain()[2];
    }

    public static Class<?> getDirectCallerClass() {
        Class<?>[] callerChain = ClassContext.INSTANCE.getCallerClassChain();
        if (callerChain.length > DIRECT_CALLER_INDEX) {
            return callerChain[DIRECT_CALLER_INDEX];
        }
        return null;
    }

    public static Class<?> getCallerClass(int level) {
        Class<?>[] callerChain = ClassContext.INSTANCE.getCallerClassChain();
        if (callerChain.length > level + DIRECT_CALLER_INDEX) {
            return callerChain[level + DIRECT_CALLER_INDEX];
        }
        return null;
    }

    public static Class<?>[] getCallerClassChain() {
        return Arrays.stream(ClassContext.INSTANCE.getCallerClassChain()).skip(DIRECT_CALLER_INDEX).toArray(Class<?>[]::new);
    }
}
