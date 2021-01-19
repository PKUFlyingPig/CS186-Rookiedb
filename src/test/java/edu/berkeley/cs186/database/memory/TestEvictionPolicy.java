package edu.berkeley.cs186.database.memory;

import edu.berkeley.cs186.database.categories.Proj99Tests;
import edu.berkeley.cs186.database.categories.SystemTests;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

@Category({Proj99Tests.class, SystemTests.class})
public class TestEvictionPolicy {
    private BufferFrame[] frames;
    private BufferFrame[] placeholderFrames;

    private class TestFrame extends BufferFrame {
        private int index;
        private TestFrame(int index) {
            this.index = index;
        }

        @Override
        public String toString() {
            return "Frame #" + index;
        }

        @Override
        boolean isValid() {
            return false;
        }

        @Override
        long getPageNum() {
            return 0;
        }

        @Override
        void flush() {
        }

        @Override
        void readBytes(short position, short num, byte[] buf) {
        }

        @Override
        void writeBytes(short position, short num, byte[] buf) {
        }

        @Override
        long getPageLSN() {
            return 0;
        }

        @Override
        void setPageLSN(long pageLSN) {
        }

        @Override
        BufferFrame requestValidFrame() {
            return null;
        }
    }

    @Before
    public void beforeEach() {
        this.frames = new BufferFrame[8];
        this.placeholderFrames = new BufferFrame[8];
        for (int i = 0; i < this.frames.length; ++i) {
            this.frames[i] = new TestFrame(i);
            this.placeholderFrames[i] = new TestFrame(-i);
            this.placeholderFrames[i].pin();
        }
    }

    @Test
    public void testLRUPolicy() {
        EvictionPolicy policy = new LRUEvictionPolicy();
        policy.init(frames[0]); policy.hit(frames[0]);
        policy.init(frames[1]); policy.hit(frames[1]);
        policy.init(frames[2]); policy.hit(frames[2]);
        policy.init(frames[3]); policy.hit(frames[3]);

        assertEquals(frames[0], policy.evict(new BufferFrame[] {frames[0], frames[1], frames[2], frames[3]}));
        policy.cleanup(frames[0]);

        policy.init(frames[4]); policy.hit(frames[4]);
        policy.hit(frames[1]);
        frames[3].pin();

        assertEquals(frames[2], policy.evict(new BufferFrame[] {frames[4], frames[1], frames[2], frames[3]}));
        policy.cleanup(frames[2]);

        policy.init(frames[5]); policy.hit(frames[5]);

        assertEquals(frames[4], policy.evict(new BufferFrame[] {frames[4], frames[1], frames[5], frames[3]}));
        policy.cleanup(frames[4]);

        policy.init(frames[6]); policy.hit(frames[6]);

        frames[3].unpin();

        assertEquals(frames[3], policy.evict(new BufferFrame[] {frames[6], frames[1], frames[5], frames[3]}));
        policy.cleanup(frames[3]);

        policy.init(frames[7]); policy.hit(frames[7]);

        policy.hit(frames[5]);

        assertEquals(frames[1], policy.evict(new BufferFrame[] {frames[6], frames[1], frames[5], frames[7]}));
        policy.cleanup(frames[1]);

        policy.init(frames[3]); policy.hit(frames[3]);

        frames[3].pin();

        assertEquals(frames[6], policy.evict(new BufferFrame[] {frames[6], frames[3], frames[5], frames[7]}));
        policy.cleanup(frames[6]);
        assertEquals(frames[7], policy.evict(new BufferFrame[] {placeholderFrames[0], frames[3], frames[5], frames[7]}));
        policy.cleanup(frames[7]);
        assertEquals(frames[5], policy.evict(new BufferFrame[] {placeholderFrames[0], frames[3], frames[5], placeholderFrames[3]}));
        policy.cleanup(frames[5]);
        boolean exceptionThrown = false;
        try {
            policy.evict(new BufferFrame[] {placeholderFrames[0], frames[3], placeholderFrames[2], placeholderFrames[3]});
        } catch (IllegalStateException e) {
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);

        frames[3].unpin();
        assertEquals(frames[3], policy.evict(new BufferFrame[] {placeholderFrames[0], frames[3], placeholderFrames[2], placeholderFrames[3]}));
        policy.cleanup(frames[3]);
    }

    @Test
    public void testClockPolicy() {
        EvictionPolicy policy = new ClockEvictionPolicy();
        policy.init(frames[0]); policy.hit(frames[0]);
        policy.init(frames[1]); policy.hit(frames[1]);
        policy.init(frames[2]); policy.hit(frames[2]);
        policy.init(frames[3]); policy.hit(frames[3]);

        assertEquals(frames[0], policy.evict(new BufferFrame[] {frames[0], frames[1], frames[2], frames[3]}));
        policy.cleanup(frames[0]);

        policy.init(frames[4]); policy.hit(frames[4]);

        policy.hit(frames[1]);
        frames[3].pin();

        assertEquals(frames[2], policy.evict(new BufferFrame[] {frames[4], frames[1], frames[2], frames[3]}));
        policy.cleanup(frames[2]);

        policy.init(frames[5]); policy.hit(frames[5]);

        assertEquals(frames[1], policy.evict(new BufferFrame[] {frames[4], frames[1], frames[5], frames[3]}));
        policy.cleanup(frames[1]);

        policy.init(frames[6]); policy.hit(frames[6]);

        frames[3].unpin();

        assertEquals(frames[3], policy.evict(new BufferFrame[] {frames[4], frames[6], frames[5], frames[3]}));
        policy.cleanup(frames[3]);

        policy.init(frames[7]); policy.hit(frames[7]);

        policy.hit(frames[4]);

        assertEquals(frames[5], policy.evict(new BufferFrame[] {frames[4], frames[6], frames[5], frames[7]}));
        policy.cleanup(frames[5]);

        policy.init(frames[2]); policy.hit(frames[2]);

        frames[2].pin();

        assertEquals(frames[4], policy.evict(new BufferFrame[] {frames[4], frames[6], frames[2], frames[7]}));
        policy.cleanup(frames[4]);
        assertEquals(frames[6], policy.evict(new BufferFrame[] {placeholderFrames[0], frames[6], frames[2], frames[7]}));
        policy.cleanup(frames[6]);
        assertEquals(frames[7], policy.evict(new BufferFrame[] {placeholderFrames[0], placeholderFrames[1], frames[2], frames[7]}));
        policy.cleanup(frames[7]);
        boolean exceptionThrown = false;
        try {
            policy.evict(new BufferFrame[] {placeholderFrames[0], placeholderFrames[1], frames[2], placeholderFrames[3]});
        } catch (IllegalStateException e) {
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);

        frames[2].unpin();
        assertEquals(frames[2], policy.evict(new BufferFrame[] {placeholderFrames[0], placeholderFrames[1], frames[2], placeholderFrames[3]}));
        policy.cleanup(frames[2]);
    }
}
