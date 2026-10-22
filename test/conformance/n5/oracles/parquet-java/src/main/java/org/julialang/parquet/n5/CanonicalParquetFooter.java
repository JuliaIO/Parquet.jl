package org.julialang.parquet.n5;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.Util;

final class CanonicalParquetFooter {
  private static final byte[] MAGIC = new byte[] {'P', 'A', 'R', '1'};
  private static final int TRAILER_SIZE = 8;

  private CanonicalParquetFooter() {}

  static void canonicalize(Path path) throws IOException {
    byte[] originalFile = Files.readAllBytes(path);
    Footer original = readFooter(originalFile);
    FileMetaData canonicalMetadata = original.metadata.deepCopy();
    sortAllEncodings(canonicalMetadata);
    assertOnlyEncodingOrderChanged(original.metadata, canonicalMetadata);
    byte[] canonicalFooter = writeMetadata(canonicalMetadata);
    if (canonicalFooter.length != original.bytes.length) {
      throw new IOException("canonical footer length changed");
    }
    byte[] canonicalFile = Arrays.copyOf(originalFile, originalFile.length);
    System.arraycopy(canonicalFooter, 0, canonicalFile, original.start, canonicalFooter.length);
    if (!Arrays.equals(
        Arrays.copyOfRange(originalFile, 0, original.start),
        Arrays.copyOfRange(canonicalFile, 0, original.start))) {
      throw new IOException("canonicalization changed Parquet data or page bytes");
    }
    Footer reparsed = readFooter(canonicalFile);
    if (!canonicalMetadata.equals(reparsed.metadata)) {
      throw new IOException("canonical footer changed during serialization");
    }
    assertOnlyEncodingOrderChanged(original.metadata, reparsed.metadata);
    if (Arrays.equals(originalFile, canonicalFile)) {
      return;
    }
    replace(path, canonicalFile);
    byte[] committed = Files.readAllBytes(path);
    if (!Arrays.equals(canonicalFile, committed)) {
      throw new IOException("committed canonical Parquet file changed");
    }
  }

  static List<Encoding> sortedEncodings(List<Encoding> encodings) {
    if (encodings == null) {
      throw new IllegalArgumentException("missing encoding list");
    }
    List<Encoding> result = new ArrayList<>(encodings);
    result.sort(Comparator.comparingInt(Encoding::getValue));
    return result;
  }

  private static Footer readFooter(byte[] file) throws IOException {
    if (file.length < MAGIC.length + TRAILER_SIZE) {
      throw new IOException("Parquet file is too short");
    }
    requireMagic(file, 0);
    requireMagic(file, file.length - MAGIC.length);
    int footerLength = ByteBuffer.wrap(file, file.length - TRAILER_SIZE, Integer.BYTES)
        .order(ByteOrder.LITTLE_ENDIAN)
        .getInt();
    if (footerLength < 0 || footerLength > file.length - MAGIC.length - TRAILER_SIZE) {
      throw new IOException("invalid Parquet footer length: " + footerLength);
    }
    int footerStart = file.length - TRAILER_SIZE - footerLength;
    byte[] footer = Arrays.copyOfRange(file, footerStart, footerStart + footerLength);
    ByteArrayInputStream input = new ByteArrayInputStream(footer);
    FileMetaData metadata = Util.readFileMetaData(input);
    if (input.available() != 0) {
      throw new IOException("Parquet footer has trailing Thrift bytes");
    }
    byte[] serialized = writeMetadata(metadata);
    if (!Arrays.equals(footer, serialized)) {
      throw new IOException("Parquet footer is not an exact Compact Thrift round trip");
    }
    return new Footer(footerStart, footer, metadata);
  }

  private static byte[] writeMetadata(FileMetaData metadata) throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    Util.writeFileMetaData(metadata, output);
    return output.toByteArray();
  }

  private static void sortAllEncodings(FileMetaData metadata) throws IOException {
    if (metadata.getRow_groups() == null) {
      throw new IOException("Parquet footer has no row groups");
    }
    for (RowGroup rowGroup : metadata.getRow_groups()) {
      if (rowGroup.getColumns() == null) {
        throw new IOException("Parquet row group has no columns");
      }
      for (ColumnChunk column : rowGroup.getColumns()) {
        ColumnMetaData columnMetadata = column.getMeta_data();
        if (columnMetadata == null || columnMetadata.getEncodings() == null) {
          throw new IOException("Parquet column has no encoding metadata");
        }
        List<Encoding> before = columnMetadata.getEncodings();
        List<Encoding> sorted = sortedEncodings(before);
        if (before.size() != sorted.size()) {
          throw new IOException("canonicalization changed encoding count");
        }
        columnMetadata.setEncodings(sorted);
      }
    }
  }

  private static void assertOnlyEncodingOrderChanged(
      FileMetaData original, FileMetaData candidate) throws IOException {
    FileMetaData expected = original.deepCopy();
    sortAllEncodings(expected);
    if (!expected.equals(candidate)) {
      throw new IOException("canonicalization changed a non-encoding footer field");
    }
  }

  private static void replace(Path path, byte[] bytes) throws IOException {
    Path absolute = path.toAbsolutePath().normalize();
    Path temporary = Files.createTempFile(
        absolute.getParent(), "." + absolute.getFileName(), ".canonical");
    boolean committed = false;
    try {
      Files.write(temporary, bytes, StandardOpenOption.TRUNCATE_EXISTING);
      try {
        Files.move(
            temporary,
            absolute,
            StandardCopyOption.ATOMIC_MOVE,
            StandardCopyOption.REPLACE_EXISTING);
      } catch (AtomicMoveNotSupportedException error) {
        Files.move(temporary, absolute, StandardCopyOption.REPLACE_EXISTING);
      }
      committed = true;
    } finally {
      if (!committed) {
        Files.deleteIfExists(temporary);
      }
    }
  }

  private static void requireMagic(byte[] file, int offset) throws IOException {
    for (int index = 0; index < MAGIC.length; index++) {
      if (file[offset + index] != MAGIC[index]) {
        throw new IOException("invalid Parquet magic");
      }
    }
  }

  private static final class Footer {
    final int start;
    final byte[] bytes;
    final FileMetaData metadata;

    Footer(int start, byte[] bytes, FileMetaData metadata) {
      this.start = start;
      this.bytes = bytes;
      this.metadata = metadata;
    }
  }
}
