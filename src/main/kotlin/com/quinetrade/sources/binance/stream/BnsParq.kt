package com.quintrade.sources.binance.stream

import org.apache.parquet.io.OutputFile
import org.apache.parquet.io.PositionOutputStream
import java.io.OutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption

class LocalOutputFile(private val path: Path) : OutputFile {
    override fun create(blockSizeHint: Long): PositionOutputStream =
        FilePositionOutputStream(Files.newOutputStream(path,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE))

    override fun createOrOverwrite(blockSizeHint: Long): PositionOutputStream = create(blockSizeHint)

    override fun supportsBlockSize(): Boolean = false
    override fun defaultBlockSize(): Long = 0
}

private class FilePositionOutputStream(private val out: OutputStream) : PositionOutputStream() {
    private var pos = 0L
    override fun write(b: Int) {
        out.write(b)
        pos++
    }
    override fun write(b: ByteArray, off: Int, len: Int) {
        out.write(b, off, len)
        pos += len
    }
    override fun getPos(): Long = pos
    override fun close() = out.close()
}

