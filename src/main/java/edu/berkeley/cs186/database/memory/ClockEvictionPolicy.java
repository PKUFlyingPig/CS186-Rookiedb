package edu.berkeley.cs186.database.memory;

/**
 * Implementation of clock eviction policy, which works by adding a reference
 * bit to each frame, and running the algorithm.
 */
public class ClockEvictionPolicy implements EvictionPolicy {
    private int arm;

    private static final Object ACTIVE = true;
    // null not false, because default tag (before this class ever sees a frame) is null.
    private static final Object INACTIVE = null;

    public ClockEvictionPolicy() {
        this.arm = 0;
    }

    /**
     * Called to initiaize a new buffer frame.
     * @param frame new frame to be initialized
     */
    @Override
    public void init(BufferFrame frame) {}

    /**
     * Called when a frame is hit.
     * @param frame Frame object that is being read from/written to
     */
    @Override
    public void hit(BufferFrame frame) {
        frame.tag = ACTIVE;
    }

    /**
     * Called when a frame needs to be evicted.
     * @param frames Array of all frames (same length every call)
     * @return index of frame to be evicted
     * @throws IllegalStateException if everything is pinned
     */
    @Override
    public BufferFrame evict(BufferFrame[] frames) {
        int iters = 0;
        // loop around the frames looking for a frame that has bit 0
        // iters is used to ensure that we don't loop forever - after two
        // passes through the frames, every frame has bit 0, so if we still haven't
        // found a good page to evict, everything is pinned.
        while ((frames[this.arm].tag == ACTIVE || frames[this.arm].isPinned()) &&
                iters < 2 * frames.length) {
            frames[this.arm].tag = INACTIVE;
            this.arm = (this.arm + 1) % frames.length;
            ++iters;
        }
        if (iters == 2 * frames.length) {
            throw new IllegalStateException("cannot evict - everything pinned");
        }
        BufferFrame evicted = frames[this.arm];
        this.arm = (this.arm + 1) % frames.length;
        return evicted;
    }

    /**
     * Called when a frame is removed, either because it
     * was returned from a call to evict, or because of other constraints
     * (e.g. if the page is deleted on disk).
     * @param frame frame being removed
     */
    @Override
    public void cleanup(BufferFrame frame) {}
}
