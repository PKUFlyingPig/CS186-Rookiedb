package edu.berkeley.cs186.database.memory;

/**
 * Interface for eviction policies for the buffer manager.
 */
public interface EvictionPolicy {
    /**
     * Called to initiaize a new buffer frame.
     * @param frame new frame to be initialized
     */
    void init(BufferFrame frame);

    /**
     * Called when a frame is hit.
     * @param frame Frame object that is being read from/written to
     */
    void hit(BufferFrame frame);

    /**
     * Called when a frame needs to be evicted.
     * @param frames Array of all frames (same length every call)
     * @return index of frame to be evicted
     * @throws IllegalStateException if everything is pinned
     */
    BufferFrame evict(BufferFrame[] frames);

    /**
     * Called when a frame is removed, either because it
     * was returned from a call to evict, or because of other constraints
     * (e.g. if the page is deleted on disk).
     * @param frame frame being removed
     */
    void cleanup(BufferFrame frame);
}
