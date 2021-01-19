package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.common.Pair;

import java.util.*;

import static java.util.stream.Collectors.toList;

/**
 * This class represents the full name of a resource. The name of a resource is
 * an ordered tuple of integers, and any subsequence of the tuple starting with
 * the first element is the name of a resource higher up on the hierarchy. For
 * debugging aid, we attach a string to each integer (which is only used in
 * toString()).
 *
 * For example, a page may have the name (0, 3, 10), where 3 is the table's
 * partition number and 10 is the page number. We store this as the list
 * [("database, 0), ("Students", 3), ("10", 10)], and its ancestors on the
 * hierarchy would be [("database", 0)] (which represents the entire database),
 * and [("database", 0), ("Students", 3)] (which represents the Students table,
 * of which this is a page of).
 */
public class ResourceName {
    private final List<Pair<String, Long>> names;
    private final int hash;

    public ResourceName(Pair<String, Long> name) {
        this(Collections.singletonList(name));
    }

    private ResourceName(List<Pair<String, Long>> names) {
        this.names = new ArrayList<>(names);
        this.hash = names.stream().map(x -> x == null ? null : x.getSecond()).collect(toList()).hashCode();
    }

    /**
     * @param parent This resource's parent, or null if this resource has no parent
     * @param name The name of this resource.
     */
    ResourceName(ResourceName parent, Pair<String, Long> name) {
        names = new ArrayList<>(parent.names);
        names.add(name);
        this.hash = names.stream().map(x -> x == null ? null : x.getSecond()).collect(toList()).hashCode();
    }

    /**
     * @return null if this resource has no parent, a copy of this resource's
     * parent ResourceName otherwise.
     */
    ResourceName parent() {
        if (names.size() > 1) {
            return new ResourceName(names.subList(0, names.size() - 1));
        }
        return null;
    }

    /**
     * @return true if this resource is a descendant of `other`, false otherwise
     */
    boolean isDescendantOf(ResourceName other) {
        if (other.names.size() >= names.size()) {
            return false;
        }
        Iterator<Pair<String, Long>> mine = names.iterator();
        Iterator<Pair<String, Long>> others = other.names.iterator();
        while (others.hasNext()) {
            if (!mine.next().getSecond().equals(others.next().getSecond())) {
                return false;
            }
        }
        return true;
    }

    /**
     * @return this resource's "current" name, e.g. for the resource:
     * - [("database, 0), ("Students", 3), ("10", 10)]
     * this method would return: ("10", 10)
     */
    Pair<String, Long> getCurrentName() {
        return names.get(names.size() - 1);
    }

    /**
     * @return this resource's names, e.g. a list like the following:
     * - [("database, 0), ("Students", 3), ("10", 10)]
     */
    List<Pair<String, Long>> getNames() {
        return names;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;
        if (!(o instanceof ResourceName)) return false;
        ResourceName other = (ResourceName) o;
        return this.names.equals(other.names);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public String toString() {
        StringBuilder rn = new StringBuilder(names.get(0).getFirst());
        for (int i = 1; i < names.size(); ++i) {
            rn.append('/').append(names.get(i).getFirst());
        }
        return rn.toString();
    }
}

