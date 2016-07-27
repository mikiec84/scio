/*
 * Copyright (c) 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.google.cloud.dataflow.sdk.io.hdfs;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.io.BoundedSource;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import javax.annotation.Nullable;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class NewHDFSFileSource<T, K, V> extends BoundedSource<T> {
  private static final long serialVersionUID = 0L;

  private final String filepattern;
  private final Class<? extends FileInputFormat<K, V>> formatClass;
  private final Class<K> keyClass;
  private final Class<V> valueClass;
  private final SerializableSplit serializableSplit;
  private final Coder<T> coder;
  private final SerializableFunction<KV<K, V>, T> converter;
  private final boolean validate;

  NewHDFSFileSource(String filepattern,
                    Class<? extends FileInputFormat<K, V>> formatClass,
                    Class<K> keyClass,
                    Class<V> valueClass,
                    SerializableSplit serializableSplit,
                    Coder<T> coder,
                    SerializableFunction<KV<K, V>, T> converter,
                    boolean validate) {
    this.filepattern = filepattern;
    this.formatClass = formatClass;
    this.keyClass = keyClass;
    this.valueClass = valueClass;
    this.serializableSplit = serializableSplit;
    this.coder = coder;
    this.converter = converter;
    this.validate = validate;
  }

  // =======================================================================
  // BoundedSource
  // =======================================================================

  @Override
  public List<? extends BoundedSource<T>> splitIntoBundles(
      long desiredBundleSizeBytes,
      PipelineOptions options) throws Exception {
    if (serializableSplit == null) {
      return Lists.transform(computeSplits(desiredBundleSizeBytes),
          new Function<InputSplit, BoundedSource<T>>() {
            @Override
            public BoundedSource<T> apply(@Nullable InputSplit inputSplit) {
              return new NewHDFSFileSource<>(
                  filepattern, formatClass, keyClass, valueClass,
                  new SerializableSplit(inputSplit),
                  coder, converter, validate);
            }
          });
    } else {
      return ImmutableList.of(this);
    }
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    long size = 0;
    Job job = Job.getInstance(); // new instance
    for (FileStatus st : listStatus(createFormat(job), job)) {
      size += st.getLen();
    }
    return size;
  }

  @Override
  public boolean producesSortedKeys(PipelineOptions options) throws Exception {
    return false;
  }

  @Override
  public BoundedReader<T> createReader(PipelineOptions options) throws IOException {
    // FIXME: implement
    return null;
  }

  @Override
  public void validate() {
    // FIXME: implement
  }

  @Override
  public Coder<T> getDefaultOutputCoder() {
    return coder;
  }

  // =======================================================================
  // Helpers
  // =======================================================================

  private List<InputSplit> computeSplits(long desiredBundleSizeBytes) throws IOException,
      IllegalAccessException, InstantiationException {
    Job job = Job.getInstance();
    FileInputFormat.setMinInputSplitSize(job, desiredBundleSizeBytes);
    FileInputFormat.setMaxInputSplitSize(job, desiredBundleSizeBytes);
    return createFormat(job).getSplits(job);
  }

  private FileInputFormat<K, V> createFormat(Job job)
      throws IOException, IllegalAccessException, InstantiationException {
    Path path = new Path(filepattern);
    FileInputFormat.addInputPath(job, path);
    return formatClass.newInstance();
  }

  private List<FileStatus> listStatus(FileInputFormat<K, V> format, Job job)
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    // FileInputFormat#listStatus is protected, so call using reflection
    Method listStatus = FileInputFormat.class.getDeclaredMethod("listStatus", JobContext.class);
    listStatus.setAccessible(true);
    @SuppressWarnings("unchecked")
    List<FileStatus> stat = (List<FileStatus>) listStatus.invoke(format, job);
    return stat;
  }

  // =======================================================================
  // SerializableSplit
  // =======================================================================

  /**
   * A wrapper to allow Hadoop {@link org.apache.hadoop.mapreduce.InputSplit}s to be
   * serialized using Java's standard serialization mechanisms. Note that the InputSplit
   * has to be Writable (which most are).
   */
  public static class SerializableSplit implements Externalizable {
    private static final long serialVersionUID = 0L;

    private InputSplit split;

    public SerializableSplit(InputSplit split) {
      checkArgument(split instanceof Writable, "Split is not writable: %s", split);
      this.split = split;
    }

    public InputSplit getSplit() {
      return split;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      out.writeUTF(split.getClass().getCanonicalName());
      ((Writable) split).write(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      String className = in.readUTF();
      try {
        split = (InputSplit) Class.forName(className).newInstance();
        ((Writable) split).readFields(in);
      } catch (InstantiationException | IllegalAccessException e) {
        throw new IOException(e);
      }
    }
  }

}
