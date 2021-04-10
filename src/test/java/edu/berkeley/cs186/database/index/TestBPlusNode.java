package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.TimeoutScaling;
import edu.berkeley.cs186.database.categories.Proj2Tests;
import edu.berkeley.cs186.database.categories.PublicTests;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

@Category(Proj2Tests.class)
public class TestBPlusNode {
    private static final int ORDER = 5;

    private BufferManager bufferManager;
    private BPlusTreeMetadata metadata;
    private LockContext treeContext;

    // 1 seconds max per method tested.
    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis((long) (
                1000 * TimeoutScaling.factor)));

    @Before
    public void setup() {
        DiskSpaceManager diskSpaceManager = new MemoryDiskSpaceManager();
        diskSpaceManager.allocPart(0);
        this.bufferManager = new BufferManager(diskSpaceManager, new DummyRecoveryManager(), 1024,
                new ClockEvictionPolicy());
        this.treeContext = new DummyLockContext();
        this.metadata = new BPlusTreeMetadata("test", "col", Type.intType(), ORDER,
                                              0, DiskSpaceManager.INVALID_PAGE_NUM, -1);
    }

    @After
    public void cleanup() {
        this.bufferManager.close();
    }

    @Test
    @Category(PublicTests.class)
    public void testFromBytes() {
        // Test deserialization for both leaf nodes and inner nodes
        // This test should be passing after you implement LeafNode::fromBytes

        // Leaf node.
        List<DataBox> leafKeys = new ArrayList<>();
        List<RecordId> leafRids = new ArrayList<>();
        for (int i = 0; i < 2 * ORDER; ++i) {
            leafKeys.add(new IntDataBox(i));
            leafRids.add(new RecordId(i, (short) i));
        }
        LeafNode leaf = new LeafNode(metadata, bufferManager, leafKeys, leafRids, Optional.of(42L),
                                     treeContext);

        // Inner node.
        List<DataBox> innerKeys = new ArrayList<>();
        List<Long> innerChildren = new ArrayList<>();
        for (int i = 0; i < 2 * ORDER; ++i) {
            innerKeys.add(new IntDataBox(i));
            innerChildren.add((long) i);
        }
        innerChildren.add((long) 2 * ORDER);
        InnerNode inner = new InnerNode(metadata, bufferManager, innerKeys, innerChildren,
                                        treeContext);

        long leafPageNum = leaf.getPage().getPageNum();
        long innerPageNum = inner.getPage().getPageNum();
        assertEquals(leaf, BPlusNode.fromBytes(metadata, bufferManager, treeContext, leafPageNum));
        assertEquals(inner, BPlusNode.fromBytes(metadata, bufferManager, treeContext, innerPageNum));
    }
}
