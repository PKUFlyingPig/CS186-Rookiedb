# IntelliJ setup for running tests

This document will help you set up IntelliJ for running an assignment's tests

1. Open up Run/Debug Configurations with Run > Edit Configurations.
2. Click the + button in the top left to create a new configuration, and choose JUnit from
   the dropdown. This should get you the following unnamed configuration:
   ![unnamed configuration menu](images/intellij-empty-configuration.png)
3. Fill in the fields as listed below, then press OK.
   ![filled in menu](images/intellij-filledin-configuration.png)
   - Name: Proj2 tests (or whichever assignment you're setting up)
   - Test kind: Category
   - Category: edu.berkeley.cs186.database.categories.Proj2Tests (or the category corresponding to the assignment you're setting up)
   - Search for tests: In whole project
4. You should now see Project 2 tests in the dropdown in the top right. You can run/debug this configuration to run all the Project 2 tests.
