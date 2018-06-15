package com.ucex.rad.file;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

public class DataFileTest {

    @Test
    public void test() throws Throwable {
        Path p = Files.createTempFile("DataFileTest", "");
        DataFile f = DataFile.newFile(100, 100, p);

        Assertions.assertTrue(f.write(0, b-> b.putInt(12345)));
        Assertions.assertFalse(f.write(0, b-> b.putInt(23456))); // should not be able to write to same index again
        Assertions.assertTrue(f.read(0, b-> Assertions.assertEquals(12345, b.getInt())));
        Assertions.assertFalse(f.read(0, b-> b.getInt()));  // record should have been erased

        Assertions.assertTrue(f.write(1, b-> b.putInt(3456)));
        Assertions.assertFalse(f.write(1, b-> b.putInt(4567))); // should not be able to write to same index again
        Assertions.assertTrue(f.read(1, b-> Assertions.assertEquals(3456, b.getInt())));
        Assertions.assertFalse(f.read(1, b-> b.getInt()));  // record should have been erased

        DataFile reopenedF = DataFile.openFile(p);
        Assertions.assertTrue(f.write(10, b-> b.putInt(8888)));
        Assertions.assertTrue(reopenedF.read(10, b-> Assertions.assertEquals(8888, b.getInt())));
    }
}
