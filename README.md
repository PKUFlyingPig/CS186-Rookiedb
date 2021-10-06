# RookieDB

![The official unofficial mascot of the class projects](images/derpydb-small.jpg)

## Overview

This repo contains my implementation for CS186 RookieDB projects. There is a [gitbook](https://cs186.gitbook.io/project/) for CS186 projects, but it may be updated each semester, so I cloned the 2021 spring version. You can find the projects handout that I used [here](./project-handout).

The master branch contains a bare-bones database implementation, which supports
executing simple transactions in series. This is the skeleton code you will use throughout the projects. In the assignments of
this class, you will be adding support for
B+ tree indices, efficient join algorithms, query optimization, multigranularity
locking to support concurrent execution of transactions, and database recovery.

## How to use

My implementation for each project is in the corresponding branch as follows (you can use `git branch -a` to see them), you can `git checkout branch_name` to see my implementation for each project.

| **Assignment**                                                                                | **Branch name** |
|-----------------------------------------------------------------------------------------------|---------------------|
| Skeleton code                                                | Master          |
| [Project 2: B+ Trees](https://cs186.gitbook.io/project/assignments/proj2)                     | b_plus_tree |
| [Project 3: Joins and Query Optimization](https://cs186.gitbook.io/project/assignments/proj3) | join_query_opt |
| [Project 4: Concurrency](https://cs186.gitbook.io/project/assignments/proj4)                  | concurrency |
| [Project 5: Recovery](https://cs186.gitbook.io/project/assignments/proj5)                     | recovery |

To start your Rookiedb projects journey, first clone the skeleton code (i.e. master branch) , then follow the instruction below to set up your local development environment. When you  start one specific project, you can create a new branch from master then implement it to keep your code tree clean. 

## Setting up your local development environment

You are free to use any text editor or IDE to complete the assignments, but **we
will build and test your code in a docker container with Maven**.

We recommend setting up a local development environment by installing Java
8 locally (the version our Docker container runs) and using an IDE such as
IntelliJ.

[Java 8 downloads](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)

If you have another version of Java installed, it's probably fine to use it, as
long as you do not use any features not in Java 8. You should run tests
somewhat frequently inside the container to make sure that your code works with
our setup.

To import the project into IntelliJ, make sure that you import as a Maven
project (select the pom.xml file when importing). Make sure that you can compile
your code and run tests (it's ok if there are a lot of failed tests - you
haven't begun implementing anything yet!). You should also make sure that you
can run the debugger and step through code.

## Running tests in IntelliJ

If you are using IntelliJ, and wish to run the tests for a given assignment
follow the instructions in the following document:

[IntelliJ setup](intellij-test-setup.md)

## The code

As you will be working with this codebase for the rest of the semester, it is a good idea to get familiar with it. The code is located in the `src/main/java/edu/berkeley/cs186/database` directory, while the tests are located in the `src/test/java/edu/berkeley/cs186/database directory`. The following is a brief overview of each of the major sections of the codebase.

### cli

The cli directory contains all the logic for the database's command line interface. Running the main method of CommandLineInterface.java will create an instance of the database and create a simple text interface that you can send and review the results of queries in. **The inner workings of this section are beyond the scope of the class** (although you're free to look around), you'll just need to know how to run the Command Line Interface.

#### cli/parser

The subdirectory cli/parser contains a lot of scary looking code! Don't be intimidated, this is all generated automatically from the file RookieParser.jjt in the root directory of the repo. The code here handles the logic to convert from user inputted queries (strings) into a tree of nodes representing the query (parse tree).

#### cli/visitor

The subdirectory cli/visitor contains classes that help traverse the trees created from the parser and create objects that the database can work with directly.

### common

The `common` directory contains bits of useful code and general interfaces that
are not limited to any one part of the codebase.

### concurrency

The `concurrency` directory contains a skeleton for adding multigranularity
locking to the database. You will be implementing this in Project 4.

### databox

Our database has, like most DBMS's, a type system distinct from that of the
programming language used to implement the DBMS. (Our DBMS doesn't quite provide
SQL types either, but it's modeled on a simplified version of SQL types).

The `databox` directory contains classes which represents values stored in
a database, as well as their types. The various `DataBox` classes represent
values of certain types, whereas the `Type` class represents types used in the
database.

An example:
```java
DataBox x = new IntDataBox(42); // The integer value '42'.
Type t = Type.intType();        // The type 'int'.
Type xsType = x.type();         // Get x's type, which is Type.intType().
int y = x.getInt();             // Get x's value: 42.
String s = x.getString();       // An exception is thrown, since x is not a string.
```

### index

The `index` directory contains a skeleton for implementing B+ tree indices. You
will be implementing this in Project 2.

### memory

The `memory` directory contains classes for managing the loading of data
into and out of memory (in other words, buffer management).

The `BufferFrame` class represents a single buffer frame (page in the buffer
pool) and supports pinning/unpinning and reading/writing to the buffer frame.
All reads and writes require the frame be pinned (which is often done via the
`requireValidFrame` method, which reloads data from disk if necessary, and then
returns a pinned frame for the page).

The `BufferManager` interface is the public interface for the buffer manager of
our DBMS.

The `BufferManagerImpl` class implements a buffer manager using
a write-back buffer cache with configurable eviction policy. It is responsible
for fetching pages (via the disk space manager) into buffer frames, and returns
Page objects to allow for manipulation of data in memory.

The `Page` class represents a single page. When data in the page is accessed or
modified, it delegates reads/writes to the underlying buffer frame containing
the page.

The `EvictionPolicy` interface defines a few methods that determine how the
buffer manager evicts pages from memory when necessary. Implementations of these
include the `LRUEvictionPolicy` (for LRU) and `ClockEvictionPolicy` (for clock).

### io

The `io` directory contains classes for managing data on-disk (in other words,
disk space management).

The `DiskSpaceManager` interface is the public interface for the disk space
manager of our DBMS.

The `DiskSpaceMangerImpl` class is the implementation of the disk space
manager, which maps groups of pages (partitions) to OS-level files, assigns
each page a virtual page number, and loads/writes these pages from/to disk.

### query

The `query` directory contains classes for managing and manipulating queries.

The various operator classes are query operators (pieces of a query), some of
which you will be implementing in Project 3.

The `QueryPlan` class represents a plan for executing a query (which we will be
covering in more detail later in the semester). It currently executes the query
as given (runs things in logical order, and performs joins in the order given),
but you will be implementing
a query optimizer in Project 3 to run the query in a more efficient manner.

### recovery

The `recovery` directory contains a skeleton for implementing database recovery
a la ARIES. You will be implementing this in Project 5.

### table

The `table` directory contains classes representing entire tables and records.

The `Table` class is, as the name suggests, a table in our database. See the
comments at the top of this class for information on how table data is layed out
on pages.

The `Schema` class represents the _schema_ of a table (a list of column names
and their types).

The `Record` class represents a record of a table (a single row). Records are
made up of multiple DataBoxes (one for each column of the table it belongs to).

The `RecordId` class identifies a single record in a table.


The `PageDirectory` class is an implementation of a heap file that uses a page directory.

#### table/stats

The `table/stats` directory contains classes for keeping track of statistics of
a table. These are used to compare the costs of different query plans, when you
implement query optimization in Project 4.

### Transaction.java

The `Transaction` interface is the _public_ interface of a transaction - it
contains methods that users of the database use to query and manipulate data.

This interface is partially implemented by the `AbstractTransaction` abstract
class, and fully implemented in the `Database.Transaction` inner class.

### TransactionContext.java

The `TransactionContext` interface is the _internal_ interface of a transaction -
it contains methods tied to the current transaction that internal methods
(such as a table record fetch) may utilize.

The current running transaction's transaction context is set at the beginning
of a `Database.Transaction` call (and available through the static
`getCurrentTransaction` method) and unset at the end of the call.

This interface is partially implemented by the `AbstractTransactionContext` abstract
class, and fully implemented in the `Database.TransactionContext` inner class.

### Database.java

The `Database` class represents the entire database. It is the public interface
of our database - users of our database can use it like a Java library.

All work is done in transactions, so to use the database, a user would start
a transaction with `Database#beginTransaction`, then call some of
`Transaction`'s numerous methods to perform selects, inserts, and updates.

For example:
```java
Database db = new Database("database-dir");

try (Transaction t1 = db.beginTransaction()) {
    Schema s = new Schema()
            .add("id", Type.intType())
            .add("firstName", Type.stringType(10))
            .add("lastName", Type.stringType(10));

    t1.createTable(s, "table1");

    t1.insert("table1", 1, "Jane", "Doe");
    t1.insert("table1", 2, "John", "Doe");

    t1.commit();
}

try (Transaction t2 = db.beginTransaction()) {
    // .query("table1") is how you run "SELECT * FROM table1"
    Iterator<Record> iter = t2.query("table1").execute();

    System.out.println(iter.next()); // prints [1, John, Doe]
    System.out.println(iter.next()); // prints [2, Jane, Doe]

    t2.commit();
}

db.close();
```

More complex queries can be found in
[`src/test/java/edu/berkeley/cs186/database/TestDatabase.java`](src/test/java/edu/berkeley/cs186/database/TestDatabase.java).

