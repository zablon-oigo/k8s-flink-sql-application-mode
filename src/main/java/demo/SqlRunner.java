package demo;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/** Executes a predefined SQL file as one Flink application. */
public final class SqlRunner {

    private SqlRunner() {
    }

    public static void main(String[] args) {
        try {
            run(args);
        } catch (Exception e) {
            System.err.println("SQL application failed: " + e.getMessage());
            e.printStackTrace(System.err);
            throw new RuntimeException("SQL application failed", e);
        }
    }

    static void run(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException(
                    "Expected exactly one argument: path to the SQL file");
        }

        Path sqlFile = Path.of(args[0]);
        if (!Files.isRegularFile(sqlFile) || !Files.isReadable(sqlFile)) {
            throw new IllegalArgumentException(
                    "SQL file does not exist or is not readable: " + sqlFile.toAbsolutePath());
        }

        System.out.println("Reading SQL file: " + sqlFile.toAbsolutePath());
        String sql = readSql(sqlFile);
        List<String> statements = splitStatements(sql);
        if (statements.isEmpty()) {
            throw new IllegalArgumentException("SQL file contains no executable statements");
        }
        if (!startsWithKeyword(statements.get(statements.size() - 1), "INSERT")) {
            throw new IllegalArgumentException(
                    "The final SQL statement must be INSERT INTO so the application can await its streaming job");
        }

        System.out.println("Parsed " + statements.size() + " SQL statements");
        TableEnvironment tableEnvironment = TableEnvironment.create(
                EnvironmentSettings.newInstance().inStreamingMode().build());

        for (int index = 0; index < statements.size(); index++) {
            String statement = statements.get(index);
            boolean finalStatement = index == statements.size() - 1;
            System.out.printf("Executing statement %d/%d:%n%s%n",
                    index + 1, statements.size(), statement);

            try {
                TableResult result = tableEnvironment.executeSql(statement);
                if (finalStatement) {
                    System.out.println("Streaming insert submitted; waiting for the job to finish");
                    result.await();
                } else if (startsWithKeyword(statement, "CREATE TABLE")) {
                    System.out.println("Table registered successfully");
                } else {
                    System.out.println("Statement executed successfully");
                }
            } catch (Exception e) {
                throw new RuntimeException(
                        "Statement " + (index + 1) + " failed: " + statement, e);
            }
        }
    }

    private static String readSql(Path sqlFile) throws IOException {
        return Files.readString(sqlFile, StandardCharsets.UTF_8);
    }

    static List<String> splitStatements(String sql) {
        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inSingleQuote = false;

        for (int index = 0; index < sql.length(); index++) {
            char character = sql.charAt(index);

            if (character == '\'' && inSingleQuote
                    && index + 1 < sql.length() && sql.charAt(index + 1) == '\'') {
                current.append("''");
                index++;
                continue;
            }
            if (character == '\'') {
                inSingleQuote = !inSingleQuote;
                current.append(character);
                continue;
            }
            if (character == ';' && !inSingleQuote) {
                addIfNotBlank(statements, current);
                continue;
            }
            current.append(character);
        }

        if (inSingleQuote) {
            throw new IllegalArgumentException("SQL contains an unterminated single-quoted string");
        }
        addIfNotBlank(statements, current);
        return statements;
    }

    private static void addIfNotBlank(List<String> statements, StringBuilder current) {
        String statement = current.toString().trim();
        if (!statement.isEmpty()) {
            statements.add(statement);
        }
        current.setLength(0);
    }

    private static boolean startsWithKeyword(String statement, String keyword) {
        return statement.toUpperCase(Locale.ROOT).startsWith(keyword);
    }
}
