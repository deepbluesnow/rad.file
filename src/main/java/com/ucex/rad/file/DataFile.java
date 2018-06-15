package com.ucex.rad.file;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.Consumer;

/**
 * Not completely thread safe. (Read on)
 *
 * Due to implementation limitations (for now), extra care must be taken to avoid writing into same record bucket
 * or reading from same record bucket from multiple threads at the same time.
 */
public class DataFile {

    static final int fileHeaderSize = 8;
    static final int recordHeaderSize = 4;

    static final byte used = 0;
    static final byte empty = 10;

    public final int recordSize;
    public final int maxRecords;

    final MappedByteBuffer map;

    private DataFile(int recordSize, int maxRecords, MappedByteBuffer map) {
        this.recordSize = recordSize;
        this.maxRecords = maxRecords;
        this.map = map;
    }

    public static DataFile newFile(int recordSize, int maxRecords, Path p) throws IOException {
        FileChannel c = FileChannel.open(p, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
        MappedByteBuffer map = c.map(FileChannel.MapMode.READ_WRITE, 0, (recordSize + recordHeaderSize) * maxRecords + fileHeaderSize);
        c.close();

        map.putInt(recordSize).putInt(maxRecords);

        for (int i = 0; i < maxRecords; i++) {
            map.put(i * recordSize + 8, empty);
        }
        //map.force();

        return new DataFile(recordSize, maxRecords, map);
    }

    public static DataFile openFile(Path p) throws IOException {
        try (FileChannel c = FileChannel.open(p, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
            MappedByteBuffer map = c.map(FileChannel.MapMode.READ_WRITE, 0, c.size());
            return new DataFile(map.getInt(), map.getInt(), map);
        }
    }

    /**
     * @return false if record bucket at given index is not empty
     */
    public boolean write(int index, Consumer<ByteBuffer> c) {
        if (index < 0) throw new IllegalArgumentException("negative index");
        if (index >= maxRecords) throw new IllegalArgumentException("index out of range");

        int recordStart = fileHeaderSize + index * recordSize;

        if (map.get(recordStart) == used) {
            return false;
        }

        try {
            c.accept(map
                    .duplicate()
                    .position(recordStart + recordHeaderSize)
                    .slice()
                    .limit(recordSize));
            map.put(recordStart, used);
            //map.force();
            return true;
        }
        catch (RuntimeException e) {
            throw e;
        }
    }

    /**
     * @return false if record bucket at given index is empty
     */
    public boolean read(int index, Consumer<ByteBuffer> c) {
        if (index < 0) throw new IllegalArgumentException("negative index");
        if (index >= maxRecords) throw new IllegalArgumentException("index out of range");

        int recordStart = fileHeaderSize + index * recordSize;

        if (map.get(recordStart) == empty) {
            return false;
        }

        try {
            c.accept(map
                    .duplicate()
                    .position(recordStart + recordHeaderSize)
                    .slice()
                    .limit(recordSize).asReadOnlyBuffer());
            map.put(recordStart, empty);
            //map.force();
            return true;
        }
        catch (RuntimeException e) {
            throw e;
        }
    }

    public void sync() {
        map.force();
    }

}
