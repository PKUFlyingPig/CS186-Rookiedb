package edu.berkeley.cs186.database.concurrency;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * This class represents the full name of a resource. The name of a resource is
 * an ordered tuple of strings, and any subsequence of the tuple starting with
 * the first element is the name of a resource higher up on the hierarchy.
 *
 * For example, a page may have the name ("database", "someTable", 10), where
 * "someTable" is the name of the table the page belongs to, and 10 is the page
 * number. We store this as the list ["database", "someTable", "10"] and its
 * ancestors on the hierarchy would be ["database"] (which represents the entire
 * database), and ["database", "someTable"] (which represents the the table,
 * of which this is a page of).
 */
public class ResourceName {
    private final List<String> names;

    public ResourceName(String name) {
        this(Collections.singletonList(name));
    }

    private ResourceName(List<String> names) {
        this.names = new ArrayList<>(names);
    }

    /**
     * @param parent This resource's parent, or null if this resource has no parent
     * @param name The name of this resource.
     */
    ResourceName(ResourceName parent, String name) {
        this.names = new ArrayList<>(parent.names);
        this.names.add(name);
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
        Iterator<String> mine = names.iterator();
        Iterator<String> others = other.names.iterator();
        while (others.hasNext()) {
            if (!mine.next().equals(others.next())) {
                return false;
            }
        }
        return true;
    }

    /**
     * @return this resource's names, e.g. a list like the following:
     * - ["database, "someTable", "10"]
     */
    List<String> getNames() {
        return names;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;
        if (!(o instanceof ResourceName)) return false;
        ResourceName other = (ResourceName) o;
        if (other.names.size() != this.names.size()) return false;
        for (int i = 0; i < other.names.size(); i++) {
            if (!this.names.get(i).equals(other.names.get(i))) return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return names.hashCode();
    }

    @Override
    public String toString() {
        StringBuilder rn = new StringBuilder(names.get(0));
        for (int i = 1; i < names.size(); ++i) {
            rn.append('/').append(names.get(i));
        }
        return rn.toString();
    }
}