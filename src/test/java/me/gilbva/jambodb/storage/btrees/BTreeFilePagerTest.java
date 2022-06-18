package me.gilbva.jambodb.storage.btrees;

import me.gilbva.jambodb.storage.blocks.SecurityOptions;
import me.gilbva.jambodb.storage.pager.FilePager;
import me.gilbva.jambodb.storage.types.IntegerSerializer;
import me.gilbva.jambodb.storage.types.SmallStringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.util.*;

public class BTreeFilePagerTest extends BTreeTestBase {
    @TestFactory
    public Collection<DynamicTest> testBTree() {
        List<DynamicTest> lst = new ArrayList<>();
        var opts = new SecurityOptions(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        createBTreeTests(null, "raw", lst);
        createBTreeTests(opts, "encrypted", lst);
        return lst;
    }

    public void createBTreeTests(SecurityOptions options, String prefix, List<DynamicTest> tests) {
        for (int i = 0; i < 10; i++) {
            final int size = i;
            tests.add(DynamicTest.dynamicTest("testing " + prefix + " btree size=" + size + " cache: 0", () -> doTest(size, 0, options)));
            tests.add(DynamicTest.dynamicTest("testing " + prefix + " btree size=" + size + " cache: 1", () -> doTest(size, 1, options)));
            tests.add(DynamicTest.dynamicTest("testing " + prefix + " btree size=" + size + " cache: 10_000", () -> doTest(size, 1000, options)));
        }

        for (int i = 0; i < 100; i+=30) {
            final int size = i;
            tests.add(DynamicTest.dynamicTest("testing " + prefix + " btree size=" + size + " cache: 1", () -> doTest(size, 1000, options)));
            tests.add(DynamicTest.dynamicTest("testing " + prefix + " btree size=" + size + " cache: 100", () -> doTest(size, 1000, options)));
        }

        for (int i = 10_000; i < 100_000; i += 30_000) {
            final int size = i;
            tests.add(DynamicTest.dynamicTest("testing " + prefix + " btree size=" + size + " cache: 10", () -> doTest(size, 10, options)));
            tests.add(DynamicTest.dynamicTest("testing " + prefix + " btree size=" + size + " cache: 100_000", () -> doTest(size, 100_000, options)));
        }
    }

    private void doTest(int size, int cachePages, SecurityOptions opts) throws IOException {
        var strToIntFile = Files.createTempFile("test", "jambodb");
        var intToStrFile = Files.createTempFile("test", "jambodb");

        var strToIntPager = FilePager
                .create(SmallStringSerializer.INSTANCE, IntegerSerializer.INSTANCE)
                .file(strToIntFile).cachePages(cachePages).security(opts)
                .build();
        var intToStrPager = FilePager
                .create(IntegerSerializer.INSTANCE, SmallStringSerializer.INSTANCE)
                .file(intToStrFile).cachePages(cachePages).security(opts)
                .build();
        performTest(size, strToIntPager, intToStrPager);
        int strToIntRoot = strToIntPager.root();
        int intToStrRoot = intToStrPager.root();

        assertSize(new BTree<>(strToIntPager), size);
        assertSize(new BTree<>(intToStrPager), size);

        strToIntPager = FilePager
                .open(SmallStringSerializer.INSTANCE, IntegerSerializer.INSTANCE)
                .file(strToIntFile).cachePages(cachePages).security(opts)
                .build();
        intToStrPager = FilePager
                .open(IntegerSerializer.INSTANCE, SmallStringSerializer.INSTANCE)
                .file(intToStrFile).cachePages(cachePages).security(opts)
                .build();

        Assertions.assertEquals(strToIntRoot, strToIntPager.root());
        Assertions.assertEquals(intToStrRoot, intToStrPager.root());

        assertSize(new BTree<>(strToIntPager), size);
        assertSize(new BTree<>(intToStrPager), size);

        removeAll(new BTree<>(strToIntPager), size);
        removeAll(new BTree<>(intToStrPager), size);

        strToIntPager = FilePager
                .open(SmallStringSerializer.INSTANCE, IntegerSerializer.INSTANCE)
                .file(strToIntFile).cachePages(cachePages).security(opts)
                .build();
        intToStrPager = FilePager
                .open(IntegerSerializer.INSTANCE, SmallStringSerializer.INSTANCE)
                .file(intToStrFile).cachePages(cachePages).security(opts)
                .build();
        performTest(size, strToIntPager, intToStrPager);
    }

    private void performTest(int size, FilePager<String, Integer> strToIntPager, FilePager<Integer, String> intToStrPager) throws IOException {
        var expectedStiTree = new TreeMap<String, Integer>();
        var expectedItsTree = new TreeMap<Integer, String>();

        var strToInt = new BTree<>(strToIntPager);
        var intToStr = new BTree<>(intToStrPager);

        for (int i = 0; i < size; i++) {
            var str = UUID.randomUUID().toString().substring(0, 8);
            while (expectedStiTree.containsKey(str)) {
                str = UUID.randomUUID().toString().substring(0, 8);
            }

            expectedStiTree.put(str, i);
            expectedItsTree.put(i, str);
        }

        var strQueries = Arrays.asList(new String[][]{
                {"a", "z"},
                {"0", "9"}
        });

        var intQueries = Arrays.asList(new Integer[][]{
                {0, 5},
                {6, 9}
        });

        testBTree(expectedItsTree, intToStr, intQueries);
        testBTree(expectedStiTree, strToInt, strQueries);
    }

}
