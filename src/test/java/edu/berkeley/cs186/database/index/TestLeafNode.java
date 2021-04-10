package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.TimeoutScaling;
import edu.berkeley.cs186.database.categories.HiddenTests;
import edu.berkeley.cs186.database.categories.Proj2Tests;
import edu.berkeley.cs186.database.categories.PublicTests;
import edu.berkeley.cs186.database.categories.SystemTests;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.io.MemoryDiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.ClockEvictionPolicy;
import edu.berkeley.cs186.database.recovery.DummyRecoveryManager;
import edu.berkeley.cs186.database.table.RecordId;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import java.util.*;

import static org.junit.Assert.*;

@Category(Proj2Tests.class)
public class TestLeafNode {
    private BufferManager bufferManager;
    private BPlusTreeMetadata metadata;
    private LockContext treeContext;

    // 1 second max per method tested.
    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis((long) (
                1000 * TimeoutScaling.factor)));

    private static DataBox d0 = new IntDataBox(0);
    private static DataBox d1 = new IntDataBox(1);
    private static DataBox d2 = new IntDataBox(2);
    private static DataBox d3 = new IntDataBox(3);
    private static DataBox d4 = new IntDataBox(4);

    private static RecordId r0 = new RecordId(0, (short) 0);
    private static RecordId r1 = new RecordId(1, (short) 1);
    private static RecordId r2 = new RecordId(2, (short) 2);
    private static RecordId r3 = new RecordId(3, (short) 3);
    private static RecordId r4 = new RecordId(4, (short) 4);

    @Before
    public void setup() {
        DiskSpaceManager diskSpaceManager = new MemoryDiskSpaceManager();
        diskSpaceManager.allocPart(0);
        this.bufferManager = new BufferManager(diskSpaceManager, new DummyRecoveryManager(), 1024,
                new ClockEvictionPolicy());
        this.treeContext = new DummyLockContext();
        this.metadata = null;
    }

    @After
    public void cleanup() {
        this.bufferManager.close();
    }

    // Helpers /////////////////////////////////////////////////////////////////
    private void setBPlusTreeMetadata(Type keySchema, int order) {
        this.metadata = new BPlusTreeMetadata("test", "col", keySchema, order,
                                              0, DiskSpaceManager.INVALID_PAGE_NUM, -1);
    }

    private LeafNode getEmptyLeaf(Optional<Long> rightSibling) {
        List<DataBox> keys = new ArrayList<>();
        List<RecordId> rids = new ArrayList<>();
        return new LeafNode(metadata, bufferManager, keys, rids, rightSibling, treeContext);
    }

    // Tests ///////////////////////////////////////////////////////////////////
    @Test
    @Category(PublicTests.class)
    public void testGetL() {
        setBPlusTreeMetadata(Type.intType(), 5);
        LeafNode leaf = getEmptyLeaf(Optional.empty());
        for (int i = 0; i < 10; ++i) {
            assertEquals(leaf, leaf.get(new IntDataBox(i)));
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testGetLeftmostLeafL() {
        setBPlusTreeMetadata(Type.intType(), 5);
        LeafNode leaf = getEmptyLeaf(Optional.empty());
        assertEquals(leaf, leaf.getLeftmostLeaf());
    }

    @Test
    @Category(PublicTests.class)
    public void testSmallBulkLoad() {
        // Bulk loads with 60% of a leaf's worth, then checks that the
        // leaf didn't split.
        int d = 5;
        float fillFactor = 0.8f;
        setBPlusTreeMetadata(Type.intType(), d);
        LeafNode leaf = getEmptyLeaf(Optional.empty());

        List<Pair<DataBox, RecordId>> data = new ArrayList<>();
        for (int i = 0; i < (int) Math.ceil(1.5 * d * fillFactor); ++i) {
            DataBox key = new IntDataBox(i);
            RecordId rid = new RecordId(i, (short) i);
            data.add(i, new Pair<>(key, rid));
        }

        assertFalse(leaf.bulkLoad(data.iterator(), fillFactor).isPresent());

        Iterator<RecordId> iter = leaf.scanAll();
        Iterator<Pair<DataBox, RecordId>> expected = data.iterator();
        while (iter.hasNext() && expected.hasNext()) {
            assertEquals(expected.next().getSecond(), iter.next());
        }
        assertFalse(iter.hasNext());
        assertFalse(expected.hasNext());
    }

    @Test
    @Category(PublicTests.class)
    public void testNoOverflowPuts() {
        int d = 5;
        setBPlusTreeMetadata(Type.intType(), d);
        LeafNode leaf = getEmptyLeaf(Optional.empty());

        for (int i = 0; i < 2 * d; ++i) {
            DataBox key = new IntDataBox(i);
            RecordId rid = new RecordId(i, (short) i);

            // Leaf should never overflow during the first 2d puts
            assertEquals(Optional.empty(), leaf.put(key, rid));

            for (int j = 0; j <= i; ++j) {
                // Test that all RecordIds inserted up to this point are still
                // present
                key = new IntDataBox(j);
                rid = new RecordId(j, (short) j);
                assertEquals(Optional.of(rid), leaf.getKey(key));
            }
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testNoOverflowPutsFromDisk() {
        // Requires both fromBytes and put to be implemented correctly.
        int d = 5;
        setBPlusTreeMetadata(Type.intType(), d);
        LeafNode leaf = getEmptyLeaf(Optional.empty());

        // Populate the leaf.
        for (int i = 0; i < 2 * d; ++i) {
            leaf.put(new IntDataBox(i), new RecordId(i, (short) i));
        }

        // Then read the leaf from disk.
        long pageNum = leaf.getPage().getPageNum();
        LeafNode fromDisk = LeafNode.fromBytes(metadata, bufferManager, treeContext, pageNum);

        // Check to see that we can read from disk.
        for (int i = 0; i < 2 * d; ++i) {
            IntDataBox key = new IntDataBox(i);
            RecordId rid = new RecordId(i, (short) i);
            assertEquals(Optional.of(rid), fromDisk.getKey(key));
        }
    }

    @Test(expected = BPlusTreeException.class)
    @Category(PublicTests.class)
    public void testDuplicatePut() {
        // Tests that a BPlusTreeException is thrown on duplicate put

        setBPlusTreeMetadata(Type.intType(), 4);
        LeafNode leaf = getEmptyLeaf(Optional.empty());

        // The initial insert is fine.
        leaf.put(new IntDataBox(0), new RecordId(0, (short) 0));

        // The duplicate insert should raise an exception.
        leaf.put(new IntDataBox(0), new RecordId(0, (short) 0));
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleRemoves() {
        // Puts 2d keys into the leaf, then removes and checks that they can no
        // be retrieved.
        // Requires put, get and remove to be implemented to pass.

        int d = 5;
        setBPlusTreeMetadata(Type.intType(), d);
        LeafNode leaf = getEmptyLeaf(Optional.empty());

        // Insert entries.
        for (int i = 0; i < 2 * d; ++i) {
            IntDataBox key = new IntDataBox(i);
            RecordId rid = new RecordId(i, (short) i);
            leaf.put(key, rid);
            assertEquals(Optional.of(rid), leaf.getKey(key));
        }

        // Remove entries.
        for (int i = 0; i < 2 * d; ++i) {
            IntDataBox key = new IntDataBox(i);
            leaf.remove(key);
            assertEquals(Optional.empty(), leaf.getKey(key));
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testScanAll() {
        // Inserts 10 entries into an empty leaf and tests that scanAll retrieves
        // the RecordId's in correct order.

        int d = 5;
        setBPlusTreeMetadata(Type.intType(), d);
        LeafNode leaf = getEmptyLeaf(Optional.empty());

        // Insert tuples in reverse order to make sure that scanAll is returning
        // things in sorted order.
        for (int i = 2 * d - 1; i >= 0; --i) {
            leaf.put(new IntDataBox(i), new RecordId(i, (short) i));
        }

        Iterator<RecordId> iter = leaf.scanAll();
        for (int i = 0; i < 2 * d; ++i) {
            assertTrue(iter.hasNext());
            assertEquals(new RecordId(i, (short) i), iter.next());
        }

        // Iterator should be out of values after 10 calls to iter.next()
        assertFalse(iter.hasNext());
    }

    @Test
    @Category(PublicTests.class)
    public void testScanGreaterEqual() {
        // Same as above but only checks the latter 5 values with scanGreaterEqual

        int d = 5;
        setBPlusTreeMetadata(Type.intType(), d);
        LeafNode leaf = getEmptyLeaf(Optional.empty());

        // Insert tuples in reverse order to make sure that scanAll is returning
        // things in sorted order.
        for (int i = 2 * d - 1; i >= 0; --i) {
            leaf.put(new IntDataBox(i), new RecordId(i, (short) i));
        }

        Iterator<RecordId> iter = leaf.scanGreaterEqual(new IntDataBox(5));
        for (int i = 5; i < 2 * d; ++i) {
            assertTrue(iter.hasNext());
            assertEquals(new RecordId(i, (short) i), iter.next());
        }
        assertFalse(iter.hasNext());
    }

    @Test
    @Category(SystemTests.class)
    public void testMaxOrder() {
        // Note that this white box test depend critically on the implementation
        // of toBytes and includes a lot of magic numbers that won't make sense
        // unless you read toBytes.
        assertEquals(4, Type.intType().getSizeInBytes());
        assertEquals(8, Type.longType().getSizeInBytes());
        assertEquals(10, RecordId.getSizeInBytes());
        for (int d = 0; d < 10; ++d) {
            int dd = d + 1;
            for (int i = 13 + (2 * d) * (4 + 10); i < 13 + (2 * dd) * (4 + 10); ++i) {
                assertEquals(d, LeafNode.maxOrder((short) i, Type.intType()));
            }
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testToSexp() {
        int d = 2;
        setBPlusTreeMetadata(Type.intType(), d);
        LeafNode leaf = getEmptyLeaf(Optional.empty());

        assertEquals("()", leaf.toSexp());
        leaf.put(new IntDataBox(4), new RecordId(4, (short) 4));
        assertEquals("((4 (4 4)))", leaf.toSexp());
        leaf.put(new IntDataBox(1), new RecordId(1, (short) 1));
        assertEquals("((1 (1 1)) (4 (4 4)))", leaf.toSexp());
        leaf.put(new IntDataBox(2), new RecordId(2, (short) 2));
        assertEquals("((1 (1 1)) (2 (2 2)) (4 (4 4)))", leaf.toSexp());
        leaf.put(new IntDataBox(3), new RecordId(3, (short) 3));
        assertEquals("((1 (1 1)) (2 (2 2)) (3 (3 3)) (4 (4 4)))", leaf.toSexp());
    }

    @Test
    @Category(PublicTests.class)
    public void testToAndFromBytes() {
        // Tests that leaf nodes are properly serialized and deserialized

        int d = 5;
        setBPlusTreeMetadata(Type.intType(), d);

        List<DataBox> keys = new ArrayList<>();
        List<RecordId> rids = new ArrayList<>();

        LeafNode leaf = new LeafNode(metadata, bufferManager, keys, rids, Optional.of(42L), treeContext);

        long pageNum = leaf.getPage().getPageNum();

        assertEquals(leaf, LeafNode.fromBytes(metadata, bufferManager, treeContext, pageNum));

        for (int i = 0; i < 10; i++) {
            keys.add(new IntDataBox(i));
            rids.add(new RecordId(i, (short) i));

            leaf = new LeafNode(metadata, bufferManager, keys, rids, Optional.of(42L), treeContext);

            pageNum = leaf.getPage().getPageNum();

            assertEquals(leaf, LeafNode.fromBytes(metadata, bufferManager, treeContext, pageNum));
        }
    }
}
