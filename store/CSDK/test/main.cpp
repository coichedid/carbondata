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

#include <iostream>
#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>
#include <sys/time.h>
#include <vector>
#include "../src/CarbonReader.h"
#include "../src/CarbonRow.h"
#include "../src/CarbonWriter.h"
#include "../src/CarbonSchemaReader.h"
#include "../src/Schema.h"
#include "../src/CarbonProperties.h"

using namespace std;

JavaVM *jvm;

/**
 * init jvm
 *
 * @return
 */
JNIEnv *initJVM() {
    JNIEnv *env;
    JavaVMInitArgs vm_args;
    int parNum = 2;
    int res;
    JavaVMOption options[parNum];

    options[0].optionString = "-Djava.class.path=../../sdk/target/carbondata-sdk.jar";
    options[1].optionString = "-verbose:jni";                // For debug and check the jni information
    //    options[2].optionString = "-Xmx12000m";            // change the jvm max memory size
    //    options[3].optionString = "-Xms5000m";             // change the jvm min memory size
    //    options[4].optionString = "-Djava.compiler=NONE";  // forbidden JIT
    vm_args.version = JNI_VERSION_1_8;
    vm_args.nOptions = parNum;
    vm_args.options = options;
    vm_args.ignoreUnrecognized = JNI_FALSE;

    res = JNI_CreateJavaVM(&jvm, (void **) &env, &vm_args);
    if (res < 0) {
        fprintf(stderr, "\nCan't create Java VM\n");
        exit(1);
    }

    return env;
}

/**
 * print array result
 *
 * @param env JNIEnv
 * @param arr array
 */
void printArray(JNIEnv *env, jobjectArray arr) {
    if (env->ExceptionCheck()) {
        throw env->ExceptionOccurred();
    }
    jsize length = env->GetArrayLength(arr);
    int j = 0;
    for (j = 0; j < length; j++) {
        jobject element = env->GetObjectArrayElement(arr, j);
        if (env->ExceptionCheck()) {
            throw env->ExceptionOccurred();
        }
        char *str = (char *) env->GetStringUTFChars((jstring) element, JNI_FALSE);
        printf("%s\t", str);
    }
    env->DeleteLocalRef(arr);
}

/**
 * print boolean result
 *
 * @param env JNIEnv
 * @param bool1 boolean value
 */
void printBoolean(jboolean bool1) {
    if (bool1) {
        printf("true\t");
    } else {
        printf("false\t");
    }
}

/**
 * print result of reading data
 *
 * @param env JNIEnv
 * @param reader CarbonReader object
 */
void printResultWithException(JNIEnv *env, CarbonReader reader) {
    try {
        CarbonRow carbonRow(env);
        while (reader.hasNext()) {
            jobject row = reader.readNextRow();
            carbonRow.setCarbonRow(row);
            printf("%s\t", carbonRow.getString(1));
            printf("%d\t", carbonRow.getInt(1));
            printf("%ld\t", carbonRow.getLong(2));
            printf("%s\t", carbonRow.getVarchar(1));
            printArray(env, carbonRow.getArray(0));
            printf("%d\t", carbonRow.getShort(5));
            printf("%d\t", carbonRow.getInt(6));
            printf("%ld\t", carbonRow.getLong(7));
            printf("%lf\t", carbonRow.getDouble(8));
            printBoolean(carbonRow.getBoolean(9));
            printf("%s\t", carbonRow.getDecimal(9));
            printf("%f\t", carbonRow.getFloat(11));
            printf("\n");
            env->DeleteLocalRef(row);
        }
        reader.close();
        carbonRow.close();
    } catch (jthrowable e) {
        env->ExceptionDescribe();
    }
}

/**
 * print result of reading data
 *
 * @param env JNIEnv
 * @param reader CarbonReader object
 */
void printResult(JNIEnv *env, CarbonReader reader) {
    try {
        CarbonRow carbonRow(env);
        while (reader.hasNext()) {
            jobject row = reader.readNextRow();
            carbonRow.setCarbonRow(row);
            printf("%s\t", carbonRow.getString(0));
            printf("%d\t", carbonRow.getInt(1));
            printf("%ld\t", carbonRow.getLong(2));
            printf("%s\t", carbonRow.getVarchar(3));
            printArray(env, carbonRow.getArray(4));
            printf("%d\t", carbonRow.getShort(5));
            printf("%d\t", carbonRow.getInt(6));
            printf("%ld\t", carbonRow.getLong(7));
            printf("%lf\t", carbonRow.getDouble(8));
            printBoolean(carbonRow.getBoolean(9));
            printf("%s\t", carbonRow.getDecimal(10));
            printf("%f\t", carbonRow.getFloat(11));
            printf("\n");
            env->DeleteLocalRef(row);
        }
        carbonRow.close();
        reader.close();
    } catch (jthrowable e) {
        env->ExceptionDescribe();
    }
}

/**
 * test read Schema from path
 *
 * @param env jni env
 * @return whether it is success
 */
bool readSchema(JNIEnv *env, char *Path, bool validateSchema) {
    printf("\nread Schema from Index File:\n");
    CarbonSchemaReader carbonSchemaReader(env);
    jobject schema;
    try {
        if (validateSchema) {
            schema = carbonSchemaReader.readSchema(Path, validateSchema);
        } else {
            schema = carbonSchemaReader.readSchema(Path);
        }
        Schema carbonSchema(env, schema);
        int length = carbonSchema.getFieldsLength();
        printf("schema length is:%d\n", length);
        for (int i = 0; i < length; i++) {
            printf("%d\t", i);
            printf("%s\t", carbonSchema.getFieldName(i));
            printf("%s\n", carbonSchema.getFieldDataTypeName(i));
            if (strcmp(carbonSchema.getFieldDataTypeName(i), "ARRAY") == 0) {
                printf("Array Element Type Name is:%s\n", carbonSchema.getArrayElementTypeName(i));
            }
        }
    } catch (jthrowable e) {
        env->ExceptionDescribe();
    }
    return true;
}

/**
 * test the exception when carbonRow with wrong index.
 *
 * @param env  jni env
 * @return
 */
bool tryCarbonRowException(JNIEnv *env, char *path) {
    printf("\nRead data from local without projection:\n");

    CarbonReader carbonReaderClass;
    try {
        carbonReaderClass.builder(env, path);
    } catch (runtime_error e) {
        printf("\nget exception fro builder and throw\n");
        throw e;
    }
    try {
        carbonReaderClass.build();
    } catch (jthrowable e) {
        env->ExceptionDescribe();
    }
    printResultWithException(env, carbonReaderClass);
}

/**
 * test read data from local disk, without projection
 *
 * @param env  jni env
 * @return
 */
bool readFromLocalWithoutProjection(JNIEnv *env, char *path) {
    printf("\nRead data from local without projection:\n");

    CarbonReader carbonReaderClass;
    try {
        carbonReaderClass.builder(env, path);
    } catch (runtime_error e) {
        printf("\nget exception fro builder and throw\n");
        throw e;
    }
    try {
        carbonReaderClass.build();
    } catch (jthrowable e) {
        env->ExceptionDescribe();
    }
    printResult(env, carbonReaderClass);
}

/**
 * test read data by readNextRow method
 *
 * @param env  jni env
 */
void testReadNextRow(JNIEnv *env, char *path, int printNum, char **argv, int argc, bool useVectorReader) {
    printf("\nTest next Row Performance, useVectorReader is ");
    printBoolean(useVectorReader);
    printf("\n");

    struct timeval start, build, startRead, endBatchRead, endRead;
    gettimeofday(&start, NULL);

    try {
        CarbonReader carbonReaderClass;

        carbonReaderClass.builder(env, path);
        if (argc > 1) {
            carbonReaderClass.withHadoopConf("fs.s3a.access.key", argv[1]);
            carbonReaderClass.withHadoopConf("fs.s3a.secret.key", argv[2]);
            carbonReaderClass.withHadoopConf("fs.s3a.endpoint", argv[3]);
        }
        if (!useVectorReader) {
            carbonReaderClass.withRowRecordReader();
        }
        carbonReaderClass.build();

        gettimeofday(&build, NULL);
        int time = 1000000 * (build.tv_sec - start.tv_sec) + build.tv_usec - start.tv_usec;
        double buildTime = time / 1000000.0;
        printf("\n\nbuild time is: %lf s\n\n", time / 1000000.0);

        CarbonRow carbonRow(env);
        int i = 0;

        gettimeofday(&startRead, NULL);
        jobject row;
        while (carbonReaderClass.hasNext()) {

            row = carbonReaderClass.readNextRow();
            carbonRow.setCarbonRow(row);
            carbonRow.getString(0);
            carbonRow.getString(1);
            carbonRow.getString(2);
            carbonRow.getString(3);
            carbonRow.getLong(4);
            carbonRow.getLong(5);

            i++;
            if (i > 1 && i % printNum == 0) {
                gettimeofday(&endBatchRead, NULL);

                time = 1000000 * (endBatchRead.tv_sec - startRead.tv_sec) + endBatchRead.tv_usec - startRead.tv_usec;
                printf("%d: time is %lf s, speed is %lf records/s  ", i, time / 1000000.0,
                       printNum / (time / 1000000.0));

                printf("%s\t", carbonRow.getString(0));
                printf("%s\t", carbonRow.getString(1));
                printf("%s\t", carbonRow.getString(2));
                printf("%s\t", carbonRow.getString(3));
                printf("%ld\t", carbonRow.getLong(4));
                printf("%ld\t", carbonRow.getLong(5));
                printf("\n");

                gettimeofday(&startRead, NULL);
            }
            env->DeleteLocalRef(row);
        }

        gettimeofday(&endRead, NULL);

        time = 1000000 * (endRead.tv_sec - build.tv_sec) + endRead.tv_usec - build.tv_usec;
        printf("total line is: %d,\t build time is: %lf s,\tread time is %lf s, average speed is %lf records/s  ",
               i, buildTime, time / 1000000.0, i / (time / 1000000.0));
        carbonReaderClass.close();
        carbonRow.close();
    } catch (jthrowable) {
        env->ExceptionDescribe();
    }
}

/**
 * test read data by readNextStringRow method
 *
 * @param env  jni env
 */
void testReadNextStringRow(JNIEnv *env, char *path, int printNum, char **argv, int argc, bool useVectorReader) {
    printf("\nTest next Row Performance, useVectorReader is ");
    printBoolean(useVectorReader);
    printf("\n");

    struct timeval start, build, startRead, endBatchRead, endRead;
    gettimeofday(&start, NULL);

    try {
        CarbonReader carbonReaderClass;

        carbonReaderClass.builder(env, path);
        if (argc > 1) {
            carbonReaderClass.withHadoopConf("fs.s3a.access.key", argv[1]);
            carbonReaderClass.withHadoopConf("fs.s3a.secret.key", argv[2]);
            carbonReaderClass.withHadoopConf("fs.s3a.endpoint", argv[3]);
        }
        if (!useVectorReader) {
            carbonReaderClass.withRowRecordReader();
        }
        carbonReaderClass.build();

        gettimeofday(&build, NULL);
        int time = 1000000 * (build.tv_sec - start.tv_sec) + build.tv_usec - start.tv_usec;
        double buildTime = time / 1000000.0;
        printf("\n\nbuild time is: %lf s\n\n", time / 1000000.0);

        int i = 0;
        gettimeofday(&startRead, NULL);
        jobject row;
        int sum2 = 0;
        while (carbonReaderClass.hasNext()) {

            row = carbonReaderClass.readNextStringRow();
            char *charArray = (char *) env->GetStringUTFChars((jstring) row, JNI_FALSE);
            sum2++;
            int index = 0;
            int j = 0;
            while (charArray[j] != '\0') {
                if ('\001' == charArray[j]) {
                    char *temp = new char[j - index];
                    for (int i = 0; i < j - index; i++) {
                        temp[i] = charArray[index + i];
                    }
                    index = j + 1;
                }
                j++;
            }
            char *temp = new char[j - index];
            for (int i = 0; i < j - index; i++) {
                temp[i] = charArray[index + i];
            }
            i++;
            if (i > 1 && i % printNum == 0) {
                gettimeofday(&endBatchRead, NULL);
                time = 1000000 * (endBatchRead.tv_sec - startRead.tv_sec) + endBatchRead.tv_usec - startRead.tv_usec;
                printf("%d: time is %lf s, speed is %lf records/s  ", i, time / 1000000.0,
                       printNum / (time / 1000000.0));
                printf("\n");
                gettimeofday(&startRead, NULL);
            }
            env->DeleteLocalRef(row);
        }

        gettimeofday(&endRead, NULL);
        time = 1000000 * (endRead.tv_sec - build.tv_sec) + endRead.tv_usec - build.tv_usec;
        printf("total line is: %d,\t build time is: %lf s,\tread time is %lf s, average speed is %lf records/s  ",
               i, buildTime, time / 1000000.0, i / (time / 1000000.0));
        carbonReaderClass.close();
    } catch (jthrowable) {
        env->ExceptionDescribe();
    }
}

/**
 * test read data by readNextBatchRow method
 *
 * @param env  jni env
 */
void testReadNextBatchRow(JNIEnv *env, char *path, int batchSize, int printNum, char **argv, int argc,
                          bool useVectorReader) {
    printf("\n\nTest next Batch Row Performance, useVectorReader is ");
    printBoolean(useVectorReader);
    printf("\n");

    struct timeval start, build, read;
    int sumHasNext = 0;
    int sumReadNext = 0;
    gettimeofday(&start, NULL);

    CarbonReader carbonReaderClass;

    carbonReaderClass.builder(env, path);
    if (argc > 1) {
        carbonReaderClass.withHadoopConf("fs.s3a.access.key", argv[1]);
        carbonReaderClass.withHadoopConf("fs.s3a.secret.key", argv[2]);
        carbonReaderClass.withHadoopConf("fs.s3a.endpoint", argv[3]);
    }
    if (!useVectorReader) {
        carbonReaderClass.withRowRecordReader();
    }
    carbonReaderClass.withBatch(batchSize);
    try {
        carbonReaderClass.build();
    } catch (jthrowable e) {
        env->ExceptionDescribe();
    }

    gettimeofday(&build, NULL);
    int time = 1000000 * (build.tv_sec - start.tv_sec) + build.tv_usec - start.tv_usec;
    double buildTime = time / 1000000.0;
    printf("\n\nbuild time is: %lf s\n\n", time / 1000000.0);

    CarbonRow carbonRow(env);
    int i = 0;
    struct timeval startHasNext, startReadNextBatchRow, endReadNextBatchRow, endRead;
    gettimeofday(&startHasNext, NULL);

    while (carbonReaderClass.hasNext()) {

        gettimeofday(&startReadNextBatchRow, NULL);
        jobjectArray batch = carbonReaderClass.readNextBatchRow();
        if (env->ExceptionCheck()) {
            env->ExceptionDescribe();
        }
        gettimeofday(&endReadNextBatchRow, NULL);

        jsize length = env->GetArrayLength(batch);
        if (i + length > printNum - 1) {
            for (int j = 0; j < length; j++) {
                i++;
                jobject row = env->GetObjectArrayElement(batch, j);
                carbonRow.setCarbonRow(row);
                carbonRow.getString(0);
                carbonRow.getString(1);
                carbonRow.getString(2);
                carbonRow.getString(3);
                carbonRow.getLong(4);
                carbonRow.getLong(5);
                if (i > 1 && i % printNum == 0) {
                    gettimeofday(&read, NULL);

                    double hasNextTime = 1000000 * (startReadNextBatchRow.tv_sec - startHasNext.tv_sec) +
                                         startReadNextBatchRow.tv_usec - startHasNext.tv_usec;

                    double readNextBatchTime = 1000000 * (endReadNextBatchRow.tv_sec - startReadNextBatchRow.tv_sec) +
                                               endReadNextBatchRow.tv_usec - startReadNextBatchRow.tv_usec;

                    time = 1000000 * (read.tv_sec - startHasNext.tv_sec) + read.tv_usec - startHasNext.tv_usec;
                    sumHasNext = sumHasNext + hasNextTime;
                    sumReadNext = sumReadNext + readNextBatchTime;
                    printf("%d: time is %lf s, speed is %lf records/s, hasNext time is %lf s,readNextBatchRow time is %lf s ",
                           i, time / 1000000.0, printNum / (time / 1000000.0), hasNextTime / 1000000.0,
                           readNextBatchTime / 1000000.0);
                    gettimeofday(&startHasNext, NULL);
                    printf("%s\t", carbonRow.getString(0));
                    printf("%s\t", carbonRow.getString(1));
                    printf("%s\t", carbonRow.getString(2));
                    printf("%s\t", carbonRow.getString(3));
                    printf("%ld\t", carbonRow.getLong(4));
                    printf("%ld\t", carbonRow.getLong(5));
                    printf("\n");
                }
                env->DeleteLocalRef(row);
            }
        } else {
            i = i + length;
        }
        env->DeleteLocalRef(batch);
    }
    gettimeofday(&endRead, NULL);
    time = 1000000 * (endRead.tv_sec - build.tv_sec) + endRead.tv_usec - build.tv_usec;
    printf("total line is: %d,\t build time is: %lf s,\tread time is %lf s, average speed is %lf records/s, "
           "hasNext total time is %lf, readNextBatch total time is %lf.\n",
           i, buildTime, time / 1000000.0, i / (time / 1000000.0), sumHasNext / 1000000.0, sumReadNext / 1000000.0);
    carbonReaderClass.close();
    carbonRow.close();
}

/**
 * test read data by readNextBatchRow method
 *
 * @param env  jni env
 */
void testReadNextBatchStringRow(JNIEnv *env, char *path, int batchSize, int printNum, char **argv, int argc,
                                bool useVectorReader) {
    printf("\n\nTest next Batch Row Performance, useVectorReader is ");
    printBoolean(useVectorReader);
    printf("\n");

    struct timeval start, build;
    int sumHasNext = 0;
    int sumReadNext = 0;
    gettimeofday(&start, NULL);

    CarbonReader carbonReaderClass;

    carbonReaderClass.builder(env, path);
    if (argc > 1) {
        carbonReaderClass.withHadoopConf("fs.s3a.access.key", argv[1]);
        carbonReaderClass.withHadoopConf("fs.s3a.secret.key", argv[2]);
        carbonReaderClass.withHadoopConf("fs.s3a.endpoint", argv[3]);
    }
    if (!useVectorReader) {
        carbonReaderClass.withRowRecordReader();
    }
    carbonReaderClass.withBatch(batchSize);
    try {
        carbonReaderClass.build();
    } catch (jthrowable e) {
        env->ExceptionDescribe();
    }

    gettimeofday(&build, NULL);
    int time = 1000000 * (build.tv_sec - start.tv_sec) + build.tv_usec - start.tv_usec;
    double buildTime = time / 1000000.0;
    printf("\n\nbuild time is: %lf s\n\n", time / 1000000.0);

    int i = 0;
    struct timeval startHasNext, startBatch, startReadNextBatchRow, endReadNextBatchRow, endRead, getString;
    gettimeofday(&startHasNext, NULL);
    gettimeofday(&startBatch, NULL);

    int sum2;
    while (carbonReaderClass.hasNext()) {

        gettimeofday(&startReadNextBatchRow, NULL);
        jobject batchString = carbonReaderClass.readNextBatchStringRow();
        i = i + batchSize;
        if (env->ExceptionCheck()) {
            env->ExceptionDescribe();
        }
        gettimeofday(&endReadNextBatchRow, NULL);
        char *charArray = (char *) env->GetStringUTFChars((jstring) batchString, JNI_FALSE);
        string str = charArray;
        for (int j = 0; j < str.length(); ++j) {
            char s2 = '\002';
            char s3 = charArray[j];
            if (s3 == s2) {
                sum2++;
                if (sum2 > 0 && sum2 % printNum == 0) {
                    cout << sum2 << ":" << j << "\n";
                }
            }
        }
        env->DeleteLocalRef(batchString);
        gettimeofday(&startHasNext, NULL);
    }
    printf("%d", sum2);
    gettimeofday(&endRead, NULL);
    time = 1000000 * (endRead.tv_sec - build.tv_sec) + endRead.tv_usec - build.tv_usec;
    printf("\ntotal line is: %d,\t build time is: %lf s,\tread time is %lf s, average speed is %lf records/s, "
           "hasNext total time is %lf, readNextBatch total time is %lf.",
           i, buildTime, time / 1000000.0, i / (time / 1000000.0), sumHasNext / 1000000.0, sumReadNext / 1000000.0);
    carbonReaderClass.close();
}

/**
 * test read data from local disk
 *
 * @param env  jni env
 * @return
 */
bool readFromLocalWithProjection(JNIEnv *env, char *path) {
    printf("\nRead data from local:\n");

    CarbonReader reader;
    reader.builder(env, path, "test");

    char *argv[12];
    argv[0] = "stringField";
    argv[1] = "shortField";
    argv[2] = "intField";
    argv[3] = "longField";
    argv[4] = "doubleField";
    argv[5] = "boolField";
    argv[6] = "dateField";
    argv[7] = "timeField";
    argv[8] = "decimalField";
    argv[9] = "varcharField";
    argv[10] = "arrayField";
    argv[11] = "floatField";
    reader.projection(12, argv);

    try {
        reader.build();
    } catch (jthrowable e) {
        env->ExceptionDescribe();
    }

    CarbonRow carbonRow(env);
    while (reader.hasNext()) {
        jobject row = reader.readNextRow();
        carbonRow.setCarbonRow(row);

        printf("%s\t", carbonRow.getString(0));
        printf("%d\t", carbonRow.getShort(1));
        printf("%d\t", carbonRow.getInt(2));
        printf("%ld\t", carbonRow.getLong(3));
        printf("%lf\t", carbonRow.getDouble(4));
        printBoolean(carbonRow.getBoolean(5));
        printf("%d\t", carbonRow.getInt(6));
        printf("%ld\t", carbonRow.getLong(7));
        printf("%s\t", carbonRow.getDecimal(8));
        printf("%s\t", carbonRow.getVarchar(9));
        printArray(env, carbonRow.getArray(10));
        printf("%f\t", carbonRow.getFloat(11));
        printf("\n");
        env->DeleteLocalRef(row);
    }

    reader.close();
    carbonRow.close();
}


bool tryCatchException(JNIEnv *env) {
    printf("\ntry catch exception and print:\n");

    CarbonReader carbonReaderClass;
    carbonReaderClass.builder(env, "./carbondata");
    try {
        carbonReaderClass.build();
    } catch (jthrowable e) {
        env->ExceptionDescribe();
        env->ExceptionClear();
    }
    printf("\nfinished handle exception\n");
}

void testCarbonProperties(JNIEnv *env) {
    printf("%s", "test Carbon Properties:");
    CarbonProperties carbonProperties(env);
    char *key = "carbon.unsafe.working.memory.in.mb";
    printf("%s\t", carbonProperties.getProperty(key));
    printf("%s\t", carbonProperties.getProperty(key, "512"));
    carbonProperties.addProperty(key, "1024");
    printf("%s\t", carbonProperties.getProperty(key));
}

/**
 * test write data to local disk
 *
 * @param env  jni env
 * @param path file path
 * @param argc argument counter
 * @param argv argument vector
 * @return true or throw exception
 */
bool testWriteData(JNIEnv *env, char *path, int argc, char *argv[]) {

    char *jsonSchema = "[{stringField:string},{shortField:short},{intField:int},{longField:long},{doubleField:double},{boolField:boolean},{dateField:date},{timeField:timestamp},{floatField:float},{arrayField:array}]";
    try {
        CarbonWriter writer;
        writer.builder(env);
        writer.outputPath(path);
        writer.withCsvInput(jsonSchema);
        writer.writtenBy("CSDK");
        if (argc > 3) {
            writer.withHadoopConf("fs.s3a.access.key", argv[1]);
            writer.withHadoopConf("fs.s3a.secret.key", argv[2]);
            writer.withHadoopConf("fs.s3a.endpoint", argv[3]);
        }
        writer.build();

        int rowNum = 10;
        int size = 10;
        long longValue = 0;
        double doubleValue = 0;
        float floatValue = 0;
        jclass objClass = env->FindClass("java/lang/String");
        for (int i = 0; i < rowNum; ++i) {
            jobjectArray arr = env->NewObjectArray(size, objClass, 0);
            char ctrInt[10];
            gcvt(i, 10, ctrInt);

            char a[15] = "robot";
            strcat(a, ctrInt);
            jobject stringField = env->NewStringUTF(a);
            env->SetObjectArrayElement(arr, 0, stringField);

            char ctrShort[10];
            gcvt(i % 10000, 10, ctrShort);
            jobject shortField = env->NewStringUTF(ctrShort);
            env->SetObjectArrayElement(arr, 1, shortField);

            jobject intField = env->NewStringUTF(ctrInt);
            env->SetObjectArrayElement(arr, 2, intField);


            char ctrLong[10];
            gcvt(longValue, 10, ctrLong);
            longValue = longValue + 2;
            jobject longField = env->NewStringUTF(ctrLong);
            env->SetObjectArrayElement(arr, 3, longField);

            char ctrDouble[10];
            gcvt(doubleValue, 10, ctrDouble);
            doubleValue = doubleValue + 2;
            jobject doubleField = env->NewStringUTF(ctrDouble);
            env->SetObjectArrayElement(arr, 4, doubleField);

            jobject boolField = env->NewStringUTF("true");
            env->SetObjectArrayElement(arr, 5, boolField);

            jobject dateField = env->NewStringUTF(" 2019-03-02");
            env->SetObjectArrayElement(arr, 6, dateField);

            jobject timeField = env->NewStringUTF("2019-02-12 03:03:34");
            env->SetObjectArrayElement(arr, 7, timeField);

            char ctrFloat[10];
            gcvt(floatValue, 10, ctrFloat);
            floatValue = floatValue + 2;
            jobject floatField = env->NewStringUTF(ctrFloat);
            env->SetObjectArrayElement(arr, 8, floatField);

            jobject arrayField = env->NewStringUTF("Hello#World#From#Carbon");
            env->SetObjectArrayElement(arr, 9, arrayField);


            writer.write(arr);

            env->DeleteLocalRef(stringField);
            env->DeleteLocalRef(shortField);
            env->DeleteLocalRef(intField);
            env->DeleteLocalRef(longField);
            env->DeleteLocalRef(doubleField);
            env->DeleteLocalRef(floatField);
            env->DeleteLocalRef(dateField);
            env->DeleteLocalRef(timeField);
            env->DeleteLocalRef(boolField);
            env->DeleteLocalRef(arrayField);
            env->DeleteLocalRef(arr);
        }
        writer.close();

        CarbonReader carbonReader;
        carbonReader.builder(env, path);
        carbonReader.build();
        int i = 0;
        CarbonRow carbonRow(env);
        while (carbonReader.hasNext()) {
            jobject row = carbonReader.readNextRow();
            i++;
            carbonRow.setCarbonRow(row);
            printf("%s\t%d\t%ld\t", carbonRow.getString(0), carbonRow.getInt(1), carbonRow.getLong(2));
            jobjectArray array1 = carbonRow.getArray(3);
            jsize length = env->GetArrayLength(array1);
            int j = 0;
            for (j = 0; j < length; j++) {
                jobject element = env->GetObjectArrayElement(array1, j);
                char *str = (char *) env->GetStringUTFChars((jstring) element, JNI_FALSE);
                printf("%s\t", str);
            }
            printf("%d\t", carbonRow.getShort(4));
            printf("%d\t", carbonRow.getInt(5));
            printf("%ld\t", carbonRow.getLong(6));
            printf("%lf\t", carbonRow.getDouble(7));
            bool bool1 = carbonRow.getBoolean(8);
            if (bool1) {
                printf("true\t");
            } else {
                printf("false\t");
            }
            printf("%f\t\n", carbonRow.getFloat(9));
            env->DeleteLocalRef(row);
        }
        carbonReader.close();
        carbonRow.close();
    } catch (jthrowable ex) {
        env->ExceptionDescribe();
        env->ExceptionClear();
    }
}

/**
 * read data from S3
 * parameter is ak sk endpoint
 *
 * @param env jni env
 * @param argv argument vector
 * @return
 */
bool readFromS3(JNIEnv *env, char *path, char *argv[]) {
    printf("\nRead data from S3:\n");
    CarbonReader reader;

    reader.builder(env, path, "test");
    reader.withHadoopConf("fs.s3a.access.key", argv[1]);
    reader.withHadoopConf("fs.s3a.secret.key", argv[2]);
    reader.withHadoopConf("fs.s3a.endpoint", argv[3]);
    reader.build();
    printResult(env, reader);
}

/**
 * This a example for C++ interface to read carbon file
 * If you want to test read data fromS3, please input the parameter: ak sk endpoint
 *
 * @param argc argument counter
 * @param argv argument vector
 * @return
 */
int main(int argc, char *argv[]) {
    // init jvm
    JNIEnv *env;
    env = initJVM();
    char *S3WritePath = "s3a://csdk/WriterOutput/carbondata2";
    char *S3ReadPath = "s3a://csdk/WriterOutput/carbondata";

    char *smallFilePath = "../../../../resources/carbondata";
    char *path = "../../../../../../../Downloads/carbon-data-big/dir2";
    char *S3Path = "s3a://csdk/bigData/i400bs128";

    if (argc > 3) {
        // TODO: need support read schema from S3 in the future
        testWriteData(env, S3WritePath, 4, argv);
        readFromS3(env, S3ReadPath, argv);

        testReadNextRow(env, S3Path, 100000, argv, 4, false);
        testReadNextRow(env, S3Path, 100000, argv, 4, true);
        testReadNextBatchRow(env, S3Path, 100000, 100000, argv, 4, false);
        testReadNextBatchRow(env, S3Path, 100000, 100000, argv, 4, true);
    } else {
        tryCatchException(env);
        tryCarbonRowException(env, smallFilePath);
        int batch = 32000;
        int printNum = 32000;

        testCarbonProperties(env);
        testWriteData(env, "./data", 1, argv);
        testWriteData(env, "./data", 1, argv);
        readFromLocalWithoutProjection(env, smallFilePath);
        readFromLocalWithProjection(env, smallFilePath);
        readSchema(env, path, false);
        readSchema(env, path, true);

        testReadNextStringRow(env, path, printNum, argv, 0, true);
        testReadNextBatchStringRow(env, path, batch, printNum, argv, 0, true);

        testReadNextRow(env, path, printNum, argv, 0, true);
        testReadNextRow(env, path, printNum, argv, 0, false);
        testReadNextBatchRow(env, path, batch, printNum, argv, 0, true);
        testReadNextBatchRow(env, path, batch, printNum, argv, 0, false);
    }
    (jvm)->DestroyJavaVM();

    cout << "\nfinish destroy jvm";
    return 0;
}

