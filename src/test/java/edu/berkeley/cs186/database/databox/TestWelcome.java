package edu.berkeley.cs186.database.databox;

import edu.berkeley.cs186.database.categories.Proj0Tests;
import edu.berkeley.cs186.database.categories.PublicTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category({Proj0Tests.class, PublicTests.class})
public class TestWelcome {
    @Test
    public void testComplete() {
        assertEquals("welcome", new StringDataBox("welcome", 7).toString());
    }
}
