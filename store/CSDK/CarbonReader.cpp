/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "CarbonReader.h"
#include <jni.h>

jobject CarbonReader::builder(JNIEnv *env, char *path, char *tableName) {

    jniEnv = env;
    jclass carbonReaderClass = env->FindClass("org/apache/carbondata/sdk/file/CarbonReader");
    jmethodID carbonReaderBuilderID = env->GetStaticMethodID(carbonReaderClass, "builder",
                                                             "(Ljava/lang/String;Ljava/lang/String;)Lorg/apache/carbondata/sdk/file/CarbonReaderBuilder;");
    jstring jpath = env->NewStringUTF(path);
    jstring jtableName = env->NewStringUTF(tableName);
    jvalue args[2];
    args[0].l = jpath;
    args[1].l = jtableName;
    carbonReaderBuilderObject = env->CallStaticObjectMethodA(carbonReaderClass, carbonReaderBuilderID, args);
    return carbonReaderBuilderObject;
}

jobject CarbonReader::build(char *ak, char *sk, char *endpoint) {
    jclass carbonReaderBuilderClass = jniEnv->GetObjectClass(carbonReaderBuilderObject);
    jmethodID buildID = jniEnv->GetMethodID(carbonReaderBuilderClass, "build",
        "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)Lorg/apache/carbondata/sdk/file/CarbonReader;");

    jvalue args[3];
    args[0].l = jniEnv->NewStringUTF(ak);
    args[1].l = jniEnv->NewStringUTF(sk);
    args[2].l = jniEnv->NewStringUTF(endpoint);
    carbonReaderObject = jniEnv->CallObjectMethodA(carbonReaderBuilderObject, buildID, args);
    return carbonReaderObject;
}

jobject CarbonReader::build() {
    jclass carbonReaderBuilderClass = jniEnv->GetObjectClass(carbonReaderBuilderObject);
    jmethodID buildID = jniEnv->GetMethodID(carbonReaderBuilderClass, "build",
                                            "()Lorg/apache/carbondata/sdk/file/CarbonReader;");
    carbonReaderObject = jniEnv->CallObjectMethod(carbonReaderBuilderObject, buildID);
    return carbonReaderObject;
}

jboolean CarbonReader::hasNext() {
    jclass carbonReader = jniEnv->GetObjectClass(carbonReaderObject);
    jmethodID hasNextID = jniEnv->GetMethodID(carbonReader, "hasNext", "()Z");
    unsigned char hasNext = jniEnv->CallBooleanMethod(carbonReaderObject, hasNextID);
    return hasNext;
}

jobjectArray CarbonReader::readNextRow() {
    jclass carbonReader = jniEnv->GetObjectClass(carbonReaderObject);
    jmethodID readNextRow2ID = jniEnv->GetMethodID(carbonReader, "readNextRow2", "()[Ljava/lang/Object;");
    jobjectArray row = (jobjectArray) jniEnv->CallObjectMethod(carbonReaderObject, readNextRow2ID);
    return row;
}

jboolean CarbonReader::close() {
    jclass carbonReader = jniEnv->GetObjectClass(carbonReaderObject);
    jmethodID closeID = jniEnv->GetMethodID(carbonReader, "close", "()V");
    jniEnv->CallBooleanMethod(carbonReaderObject, closeID);
}