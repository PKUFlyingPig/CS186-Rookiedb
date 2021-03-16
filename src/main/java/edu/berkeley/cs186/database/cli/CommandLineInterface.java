package edu.berkeley.cs186.database.cli;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.cli.parser.ASTSQLStatementList;
import edu.berkeley.cs186.database.cli.parser.RookieParser;
import edu.berkeley.cs186.database.cli.parser.ParseException;
import edu.berkeley.cs186.database.cli.parser.TokenMgrError;
import edu.berkeley.cs186.database.cli.visitor.RookieParserVisitor;
import edu.berkeley.cs186.database.concurrency.LockManager;
import edu.berkeley.cs186.database.memory.ClockEvictionPolicy;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.query.QueryPlan;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.Table;

public class CommandLineInterface {
    private static String mascot = "\n\\|/  ___------___\n \\__|--%s______%s--|\n    |  %-9s |\n     ---______---\n";
    private static int[] version = { 1, 8, 6 }; // {major, minor, build}
    private static String label = "sp21";
    private static Random generator = new Random();

    public static void main(String args[]) throws IOException {
        // startup sequence
        boolean startup = false;
        for (int i = 1; i < args.length; i++) {
            if (args[i].equals("--startup"))
                startup = true;
        }
        if (startup)
            startup();

        // Welcome message
        System.out.printf(mascot, "o", "o", institution[generator.nextInt(institution.length)]);
        System.out.printf("\nWelcome to RookieDB (v%d.%d.%d-%s)\n", version[0], version[1], version[2], label);

        // Basic database for project 0 through 3
        Database db = new Database("demo", 25);
        
        // Use the following after completing project 4 (locking)
        // Database db = new Database("demo", 25, new LockManager());
        
        // Use the following after completing project 5 (recovery)
        // Database db = new Database("demo", 25, new LockManager(), new ClockEvictionPolicy(), true);

        db.loadDemo();

        // REPL
        Transaction currTransaction = null;
        Scanner inputScanner = new Scanner(System.in);
        String input;
        while (true) {
            try {
                input = bufferUserInput(inputScanner);
                if (input.length() == 0)
                    continue;
                if (input.startsWith("\\")) {
                    try {
                        parseMetaCommand(input, db);
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                    }
                    continue;
                }
                if (input.equals("exit")) {
                    throw new NoSuchElementException();
                }
            } catch (NoSuchElementException e) {
                // User sent termination character
                if (currTransaction != null) {
                    currTransaction.rollback();
                    currTransaction.close();
                }
                System.out.println("exit");
                db.close();
                System.out.println("Bye!"); // If MariaDB says it so can we :)
                return;
            }

            // Convert input to raw bytes
            ByteArrayInputStream stream = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
            RookieParser parser = new RookieParser(stream);
            ASTSQLStatementList node;
            try {
                node = parser.sql_stmt_list();
            } catch (ParseException | TokenMgrError e) {
                System.out.println("Parser exception: " + e.getMessage());
                continue;
            }
            RookieParserVisitor visitor = new RookieParserVisitor(db);
            try {
                node.jjtAccept(visitor, null);
                currTransaction = visitor.execute(currTransaction);
            } catch (DatabaseException e) {
                System.out.println("Database exception: " + e.getMessage());
            }
        }
    }

    public static String bufferUserInput(Scanner s) {
        int numSingleQuote = 0;
        System.out.print("=> ");
        StringBuilder result = new StringBuilder();
        boolean firstLine = true;
        do {
            String curr = s.nextLine();
            if (firstLine) {
                String trimmed = curr.trim().replaceAll("(;|\\s)*$", "");
                if (curr.length() == 0) {
                    return "";
                } else if (trimmed.startsWith("\\")) {
                    return trimmed.replaceAll("", "");
                } else if (trimmed.toLowerCase().equals("exit")) {
                    return "exit";
                }
            }
            for (int i = 0; i < curr.length(); i++) {
                if (curr.charAt(i) == '\'') {
                    numSingleQuote++;
                }
            }
            result.append(curr);

            if (numSingleQuote % 2 != 0)
                System.out.print("'> ");
            else if (!curr.trim().endsWith(";"))
                System.out.print("-> ");
            else
                break;
            firstLine = false;
        } while (true);
        return result.toString();
    }

    private static void parseMetaCommand(String input, Database db) {
        input = input.substring(1); // Shave off the initial slash
        String[] tokens = input.split("\\s+");
        String cmd = tokens[0];
        if (cmd.equals("d")) {
            if (tokens.length == 1) {
                Transaction t = db.beginTransaction();
                // The schema column of the table contains raw bytes that we don't
                // want to even attempt to display to the user. So we need to
                // project out the last column.
                QueryPlan plan = t.query("_metadata.tables");
                List<String> columnNames = Arrays.asList(
                    "table_name", "part_num", "page_num", "is_temporary"
                );
                List<String> prefixed = columnNames.stream().map(
                    s -> "_metadata.tables." + s
                ).collect(Collectors.toList());
                plan.project(prefixed);
                PrettyPrinter.printRecords(columnNames, plan.execute());
                t.close();
            } else if (tokens.length == 2) {
                String tableName = tokens[1];
                Transaction t = db.beginTransaction();
                Table table = t.getTransactionContext().getTable(tableName);
                if (table == null) {
                    System.out.printf("No table \"%s\" found.", tableName);
                    return;
                }
                System.out.printf("Table \"%s\"\n", tableName);
                Schema s = table.getSchema();
                PrettyPrinter.printSchema(s);
            }
        } else if (cmd.equals("di")) {
            Transaction t = db.beginTransaction();
            QueryOperator op = t.query("_metadata.indices").getFinalOperator();
            PrettyPrinter.printRecords(op.getSchema().getFieldNames(), op.iterator());
            t.close();
        } else {
            throw new IllegalArgumentException(String.format(
                "`%s` is not a valid metacommand",
                cmd
            ));
        }
    }

    private static String[] institution = {
            "berkeley", "berkley", "berklee", "Brocolli", "BeRKeLEy", "UC Zoom",
            "   UCB  ", "go bears", "   #1  "
    };

    private static List<String> startupMessages = Arrays
            .asList("Speaking with the buffer manager", "Saying grace hash",
                    "Parallelizing parking spaces", "Bulk loading exam preparations",
                    "Declaring functional independence", "Maintaining long distance entity-relationships" );

    private static List<String> startupProblems = Arrays
            .asList("Rebuilding air quality index", "Extinguishing B+ forest fires",
                    "Recovering from PG&E outages", "Disinfecting user inputs", "Shellsorting in-place",
                    "Distributing face masks", "Joining Zoom meetings", "Caching out of the stock market",
                    "Advising transactions to self-isolate", "Tweaking the quarantine optimizer");

    private static void startup() {
        Collections.shuffle(startupMessages);
        Collections.shuffle(startupProblems);
        System.out.printf("Starting RookieDB (v%d.%d.%d-%s)\n", version[0], version[1], version[2], label);
        sleep(100);
        for (int i = 0; i < 3; i++) {
            System.out.print(" > " + startupMessages.get(i));
            ellipses();
            sleep(100);
            if (i < 4) {
                System.out.print(" Done");
            } else {
                ellipses();
                System.out.print(" Error!");
                sleep(125);
            }
            sleep(75);
            System.out.println();
        }
        System.out.println("\nEncountered unexpected problems! Applying fixes:");
        sleep(100);
        for (int i = 0; i < 3; i++) {
            System.out.print(" > " + startupProblems.get(i));
            ellipses();
            System.out.print(" Done");
            sleep(75);
            System.out.println();
        }
        sleep(100);
        System.out.println();
        System.out.println("Initialization succeeded!");
        System.out.println();
    }

    private static void ellipses() {
        for (int i = 0; i < 3; i++) {
            System.out.print(".");
            sleep(25 + generator.nextInt(50));
        }
    }

    private static void sleep(int timeMilliseconds) {
        try {
            TimeUnit.MILLISECONDS.sleep(timeMilliseconds);
        } catch (InterruptedException e) {
            System.out.println("Interrupt signal received.");
        }
    }
}