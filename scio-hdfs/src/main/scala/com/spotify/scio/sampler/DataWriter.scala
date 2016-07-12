/*
 * Copyright 2016 Spotify AB.
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

package com.spotify.scio.sampler

import org.apache.avro.file.{CodecFactory, DataFileWriter}
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

trait DataWriter[T] {
  def write(data: Seq[T])
}

class AvroDataWriter(path: Path) extends DataWriter[GenericRecord] {
  override def write(data: Seq[GenericRecord]): Unit = {
    val schema = data.head.getSchema

    val fs = FileSystem.get(path.toUri, new Configuration)
    val stream = fs.create(path, false)
    val datumWriter = new GenericDatumWriter[GenericRecord](schema)
    val fileWriter = new DataFileWriter(datumWriter)
      .setCodec(CodecFactory.deflateCodec(CodecFactory.DEFAULT_DEFLATE_LEVEL))
      .create(schema, stream)
    data.foreach(fileWriter.append)
    fileWriter.close()
  }
}
